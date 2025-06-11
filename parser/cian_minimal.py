import json
import time
import logging
import random
import re
import os
import datetime
from typing import Tuple, List, Optional, Dict, Any
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from uuid import UUID
from core.models import Offer
from fake_useragent import UserAgent

# Используем undetected_chromedriver для обхода детектирования
import undetected_chromedriver as uc

log = logging.getLogger(__name__)

# Создаем каталог для логов и скриншотов
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cian_debug_logs")
os.makedirs(LOG_DIR, exist_ok=True)

# URL templates для полной версии сайта (не мобильной)
CIAN_MAIN_URL = "https://cian.ru/"
CIAN_DISTRICTS = "https://www.cian.ru/api/geo/get-districts-tree/?locationId=1"
CIAN_GEOCODE = "https://www.cian.ru/api/geo/geocode-cached/?request={}"
CIAN_GEOCODE_FOR_SEARCH = "https://www.cian.ru/api/geo/geocoded-for-search/"
CIAN_SALE_SEARCH = (
    "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2"
    "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3&office_type[3]=4&office_type[4]=5"
    "&office_type[5]=7&office_type[6]=11&{}"
)
CIAN_SALE_SEARCH_LAND = (
    "https://www.cian.ru/cat.php?cats[0]=commercialLandSale&deal_type=sale&engine_version=2"
    "&offer_type=offices&{}"
)
CIAN_RENT_SEARCH = (
    "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2"
    "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3&office_type[3]=4&office_type[4]=5"
    "&office_type[5]=7&office_type[6]=11&{}"
)
CIAN_RENT_SEARCH_LAND = (
    "https://www.cian.ru/cat.php?cats[0]=commercialLandRent&deal_type=rent&engine_version=2"
    "&offer_type=offices&{}"
)

# Для стандартизации адресов
address_replacements = {
    "г ": "г. ",
    "обл ": "область ",
    "г.Москва": "г. Москва",
    "р-н": "район",
    "пр-кт": "проспект",
}

# Словарь соответствия названий районов Москвы и их ID на Циан
moscow_district_name_to_cian_id = {}


def save_debug_info(url: str, html: str, driver=None, prefix="page"):
    """Сохраняет HTML страницы и скриншот для отладки."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_base = os.path.join(LOG_DIR, f"{prefix}_{timestamp}")
    
    # Сохраняем HTML
    html_filename = f"{filename_base}.html"
    with open(html_filename, "w", encoding="utf-8") as f:
        f.write(f"<!-- URL: {url} -->\n{html}")
    log.info(f"Сохранен HTML в {html_filename}")
    
    # Сохраняем скриншот, если драйвер доступен
    if driver:
        try:
            screenshot_filename = f"{filename_base}.png"
            driver.save_screenshot(screenshot_filename)
            log.info(f"Сохранен скриншот в {screenshot_filename}")
        except Exception as e:
            log.warning(f"Не удалось сохранить скриншот: {e}")


class CianParser:
    def __init__(self):
        self.driver = None
        self.first_tab = None
        self.initialize_driver()
        self.init_district_mapping()
    
    def initialize_driver(self):
        """Инициализирует драйвер Chrome для работы с Циан"""
        log.info("Инициализация драйвера Chrome...")
        
        options = uc.ChromeOptions()
        options.add_argument(f"--user-agent={UserAgent(browsers=['chrome']).random}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.page_load_strategy = "eager"
        
        try:
            # Изменено: headless=False для отладки
            self.driver = uc.Chrome(options=options, headless=False)
            self.driver.set_page_load_timeout(60)  # Увеличен таймаут
            
            # Открываем главную страницу и сохраняем первую вкладку
            self.driver.get(CIAN_MAIN_URL)
            time.sleep(3)
            self.first_tab = self.driver.current_window_handle
            
            # Добавляем куки для имитации обычного пользователя
            try:
                self.driver.add_cookie({"name": "visited_before", "value": "true"})
                self.driver.add_cookie({"name": "session_region_id", "value": "1"})
            except Exception:
                pass
                
            # Открываем новую вкладку для работы
            self.driver.switch_to.new_window("tab")
            log.info("Драйвер Chrome успешно инициализирован")
        except Exception as e:
            log.error(f"Ошибка при инициализации драйвера: {e}")
            if self.driver:
                self.driver.quit()
            raise
            
    def refresh_main_page(self):
        """Обновляет главную страницу для обхода ограничений"""
        try:
            current_tab = self.driver.current_window_handle
            self.driver.switch_to.window(self.first_tab)
            self.driver.refresh()
            time.sleep(3)
            self.driver.switch_to.window(current_tab)
        except Exception as e:
            log.warning(f"Ошибка при обновлении главной страницы: {e}")
            self.initialize_driver()
    
    def init_district_mapping(self):
        """Инициализирует маппинг районов Москвы для поиска"""
        global moscow_district_name_to_cian_id
        
        try:
            self.driver.get(CIAN_DISTRICTS)
            result = json.loads(self.driver.find_element(By.TAG_NAME, "pre").text)
            
            moscow_district_name_to_cian_id = {}
            for adm_district in result:
                if adm_district.get("type") == "Okrug":
                    for district in adm_district.get("childs", []):
                        moscow_district_name_to_cian_id[district["name"]] = district["id"]
                else:
                    moscow_district_name_to_cian_id[adm_district["name"]] = adm_district["id"]
                    
            log.info(f"Загружено {len(moscow_district_name_to_cian_id)} районов Москвы")
        except Exception as e:
            log.error(f"Ошибка при загрузке районов: {e}")
    
    def get_page(self, url):
        """Получает страницу с повторными попытками"""
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                self.driver.get(url)
                time.sleep(random.uniform(2, 4))
                
                # Имитация случайного скроллинга страницы
                for _ in range(random.randint(1, 3)):
                    self.driver.execute_script(f"window.scrollBy(0, {random.randint(300, 800)})")
                    time.sleep(random.uniform(0.5, 1.5))
                
                html = self.driver.page_source
                
                # Проверяем наличие признаков блокировки
                if any(marker in html.lower() for marker in [
                    "временно недоступна", "captcha", "подтвердите что вы не робот"
                ]):
                    log.warning(f"Обнаружены признаки блокировки на попытке {attempt+1}, обновляем сессию")
                    self.refresh_main_page()
                    time.sleep(random.uniform(3, 5))
                    continue
                    
                return html
            except Exception as e:
                log.error(f"Ошибка при получении страницы {url}: {e}")
                self.refresh_main_page()
                time.sleep(random.uniform(2, 5))
        
        log.error(f"Не удалось загрузить страницу {url} после {max_attempts} попыток")
        return None
    
    def get_json(self, url):
        """Получает JSON с указанного URL"""
        try:
            self.driver.get(url)
            time.sleep(2)
            pre = self.driver.find_element(By.TAG_NAME, "pre")
            return json.loads(pre.text)
        except Exception as e:
            log.error(f"Ошибка при получении JSON с {url}: {e}")
            return {}
    
    def post_json(self, url, body):
        """Отправляет POST запрос и получает JSON ответ"""
        try:
            self.driver.execute_script("""
            function post(path, params) {
                const form = document.createElement('form');
                form.method = 'post';
                form.action = path;
                
                for (const key in params) {
                    if (params.hasOwnProperty(key)) {
                        const hiddenField = document.createElement('input');
                        hiddenField.type = 'hidden';
                        hiddenField.name = key;
                        hiddenField.value = params[key];
                        form.appendChild(hiddenField);
                    }
                }
                
                document.body.appendChild(form);
                form.submit();
            }
            
            post(arguments[1], arguments[0]);
            """, body, url)
            
            time.sleep(3)
            pre = self.driver.find_element(By.TAG_NAME, "pre")
            return json.loads(pre.text)
        except Exception as e:
            log.error(f"Ошибка при POST запросе к {url}: {e}")
            return {}
    
    def extract_offers_from_search_page(self, search_page, search_url, lot_uuid, offer_type):
        """Извлекает объявления напрямую со страницы поиска"""
        offers = []
        
        try:
            # Метод 1: Извлечение из JSON-данных в скрипте
            match = re.search(r'window\.ca\("pageview",(\{.*?\})\)', search_page)
            if match:
                try:
                    json_str = match.group(1)
                    data = json.loads(json_str)
                    if 'products' in data and isinstance(data['products'], list):
                        for product in data['products']:
                            if 'id' not in product:
                                continue
                                
                            # Извлекаем цену
                            price = 0
                            if offer_type.startswith("sale"):
                                # Для объявлений о продаже
                                price_candidates = [
                                    product.get('price', 0),
                                    product.get('price', {}).get('value', 0) if isinstance(product.get('price'), dict) else 0
                                ]
                                price = next((p for p in price_candidates if p), 0)
                            else:
                                # Для объявлений об аренде
                                price_candidates = [
                                    product.get('price', 0),
                                    product.get('price', {}).get('value', 0) if isinstance(product.get('price'), dict) else 0
                                ]
                                price = next((p for p in price_candidates if p), 0)
                            
                            # Извлекаем площадь
                            area = 0
                            area_str = ""
                            # Пробуем несколько путей для получения площади
                            if 'features' in product and isinstance(product['features'], dict):
                                area_str = product['features'].get('area', '')
                            elif 'features' in product and isinstance(product['features'], list):
                                # Ищем характеристику с площадью
                                for feature in product['features']:
                                    if isinstance(feature, dict) and 'name' in feature and 'м²' in feature.get('name', ''):
                                        area_str = feature.get('value', '')
                                        break
                            
                            # Если не удалось найти по этим путям, ищем в общем описании
                            if not area_str and 'headline' in product and 'м²' in product['headline']:
                                area_str = product['headline']
                            
                            # Извлекаем числовое значение площади
                            if area_str:
                                area_match = re.search(r'(\d+[.,]?\d*)\s*[мm]²', area_str.replace(' ', ''))
                                if area_match:
                                    area = float(area_match.group(1).replace(',', '.'))
                            
                            # Адрес
                            address = ""
                            if 'geo' in product and isinstance(product['geo'], dict):
                                address = product['geo'].get('address', '')
                            elif 'address' in product:
                                address = product['address']
                            
                            # Создаем объект предложения только если есть все необходимые данные
                            if price > 0 and area > 0:
                                offer = Offer(
                                    id=str(product.get('id', '')),
                                    lot_uuid=lot_uuid,
                                    price=price,
                                    area=area,
                                    url=f"https://www.cian.ru/{offer_type.split()[0]}/commercial/{product.get('id', '')}/",
                                    type=offer_type.split()[0],
                                    address=address
                                )
                                offers.append(offer)
                        
                        log.info(f"Извлечено {len(offers)} объявлений из JSON-данных")
                        return offers
                except Exception as e:
                    log.warning(f"Ошибка при декодировании JSON-данных из скрипта: {str(e)}")
            
            # Метод 2: Извлечение из скриптов с CIAN_COMPONENT_DATA
            scripts = re.findall(r'window\._CIAN_COMPONENT_DATA_\s*=\s*({.*?});', search_page)
            if scripts:
                for script_text in scripts:
                    try:
                        data = json.loads(script_text)
                        offers_list = []
                        
                        if "offers" in data:
                            offers_list = data["offers"]
                        elif "value" in data and "results" in data["value"]:
                            offers_list = data["value"]["results"].get("offers", [])
                        elif "value" in data and "results" in data["value"] and "aggregatedOffers" in data["value"]["results"]:
                            offers_list = data["value"]["results"]["aggregatedOffers"]
                        
                        for offer_data in offers_list:
                            try:
                                oid = str(offer_data.get("id", 0))
                                if not oid:
                                    continue
                                
                                # Формируем URL
                                if offer_type.startswith("sale"):
                                    href = f"https://www.cian.ru/sale/commercial/{oid}/"
                                else:
                                    href = f"https://www.cian.ru/rent/commercial/{oid}/"
                                
                                # Извлекаем цену
                                price = 0
                                price_candidates = [
                                    offer_data.get("bargainTerms", {}).get("priceRur", 0),
                                    offer_data.get("price", 0),
                                    offer_data.get("priceTotalRur", 0),
                                    offer_data.get("priceTotalPerMonthRur", 0)
                                ]
                                price = next((p for p in price_candidates if p), 0)
                                
                                # Извлекаем площадь
                                area = 0
                                if "totalArea" in offer_data:
                                    area = float(offer_data["totalArea"])
                                elif "areaDetails" in offer_data and "totalArea" in offer_data["areaDetails"]:
                                    area = float(offer_data["areaDetails"]["totalArea"])
                                elif "area" in offer_data:
                                    area = float(offer_data["area"])
                                
                                # Получаем адрес
                                address = ""
                                if "geo" in offer_data and "address" in offer_data["geo"]:
                                    address = offer_data["geo"]["address"]
                                elif "address" in offer_data:
                                    address = offer_data["address"]
                                
                                if price > 0 and area > 0:
                                    offer = Offer(
                                        id=oid,
                                        lot_uuid=lot_uuid,
                                        price=price,
                                        area=area,
                                        url=href,
                                        type=offer_type.split()[0],
                                        address=address
                                    )
                                    offers.append(offer)
                            except Exception as e:
                                log.debug(f"Ошибка обработки объявления: {str(e)}")
                    except Exception as e:
                        log.debug(f"Ошибка обработки JSON: {str(e)}")
                
                if offers:
                    log.info(f"Извлечено {len(offers)} объявлений из _CIAN_COMPONENT_DATA_")
                    return offers
            
            # Метод 3: Извлечение через JavaScript в браузере
            log.info("Пытаемся извлечь данные с помощью JavaScript")
            js_offers = self.driver.execute_script("""
            try {
                // Поиск карточек объявлений по разным селекторам
                function findCards() {
                    const selectors = [
                        '[data-name="CardComponent"]', 
                        '.c-card', 
                        '.catalog-item',
                        '.offer-card',
                        'article.--card--',
                        '[data-testid="offer-card"]',
                        '.serp-item',
                        '.CardItem',
                        '.catalog-card'
                    ];
                    
                    for (const selector of selectors) {
                        const cards = document.querySelectorAll(selector);
                        if (cards && cards.length) {
                            console.log("Found cards with selector", selector, cards.length);
                            return Array.from(cards);
                        }
                    }
                    
                    // Если не нашли по селекторам, поищем ссылки
                    const links = document.querySelectorAll('a[href*="/offer/"], a[href*="/rent/"], a[href*="/sale/"]');
                    if (links && links.length) {
                        console.log("Found links", links.length);
                        return Array.from(links).map(link => link.closest('article') || link.closest('div') || link);
                    }
                    
                    return [];
                }
                
                // Извлечение цены из текста
                function extractPrice(text) {
                    if (!text) return 0;
                    const matches = text.replace(/\\s/g, '').match(/\\d+/g);
                    return matches ? parseInt(matches.join('')) : 0;
                }
                
                // Извлечение площади из текста
                function extractArea(text) {
                    if (!text) return 0;
                    const matches = text.match(/(\\d+[.,]?\\d*)[\\s]*м²/);
                    return matches ? parseFloat(matches[1].replace(',', '.')) : 0;
                }
                
                const cards = findCards();
                console.log("Total cards found:", cards.length);
                
                return cards.map(card => {
                    try {
                        // Поиск различных элементов с информацией
                        const linkElement = card.querySelector('a[href*="/offer/"], a[href*="/rent/"], a[href*="/sale/"]');
                        const href = linkElement ? linkElement.getAttribute('href') : '';
                        const id = href ? href.split('/').filter(Boolean).pop() : '';
                        
                        console.log("Processing card with href:", href);
                        
                        // Поиск цены
                        const priceSelectors = [
                            '[data-mark="MainPrice"]', 
                            '[data-name="PriceInfo"]', 
                            '.c-price', 
                            '.price', 
                            '[data-testid="price"]',
                            '.CardItem__price--foSJi',
                            '.price__price--eLjQ_'
                        ];
                        let priceEl = null;
                        for (const selector of priceSelectors) {
                            priceEl = card.querySelector(selector);
                            if (priceEl) break;
                        }
                        
                        // Если не нашли по селекторам, ищем по тексту
                        let price = 0;
                        if (priceEl) {
                            price = extractPrice(priceEl.textContent);
                            console.log("Price found:", price);
                        } else {
                            // Ищем по тексту всей карточки
                            const priceText = card.innerText.match(/\\d+\\s*\\d+\\s*\\d+\\s*₽/);
                            if (priceText) {
                                price = extractPrice(priceText[0]);
                                console.log("Price found by text:", price);
                            }
                        }
                        
                        // Поиск площади
                        const areaSelectors = [
                            '[data-mark="AreaInfo"]', 
                            '[data-name="AreaInfo"]', 
                            '.c-area', 
                            '.area', 
                            '[data-testid="area"]',
                            '.main-data--info--value',
                            '.CardItem__area--gV6bA'
                        ];
                        let areaEl = null;
                        for (const selector of areaSelectors) {
                            areaEl = card.querySelector(selector);
                            if (areaEl && areaEl.textContent.includes('м²')) break;
                        }
                        
                        // Если не нашли площадь, пробуем найти по тексту
                        let area = 0;
                        if (areaEl) {
                            area = extractArea(areaEl.textContent);
                            console.log("Area found:", area);
                        } else {
                            // Поиск по всему тексту карточки
                            const cardText = card.textContent;
                            const areaMatch = cardText.match(/(\\d+[.,]?\\d*)\\s*м²/);
                            if (areaMatch) {
                                area = parseFloat(areaMatch[1].replace(',', '.'));
                                console.log("Area found by text:", area);
                            }
                        }
                        
                        // Поиск адреса
                        const addressSelectors = [
                            '[data-name="GeoLabel"]', 
                            '[data-mark="AddressInfo"]',
                            '.c-address', 
                            '.address', 
                            '[data-testid="address"]',
                            '.location--bKKSL'
                        ];
                        let addressEl = null;
                        for (const selector of addressSelectors) {
                            addressEl = card.querySelector(selector);
                            if (addressEl) break;
                        }
                        const address = addressEl ? addressEl.textContent.trim() : '';
                        
                        return {
                            id: id,
                            url: href.startsWith('/') ? 'https://www.cian.ru' + href : href,
                            price: price,
                            area: area,
                            address: address
                        };
                    } catch (e) {
                        console.error('Error processing card:', e);
                        return null;
                    }
                }).filter(item => item && item.id && item.price > 0 && item.area > 0);
            } catch (e) {
                console.error('Global error:', e);
                return [];
            }
            """)
            
            if js_offers and len(js_offers) > 0:
                log.info(f"JavaScript метод нашел {len(js_offers)} объявлений")
                
                # Преобразуем JS-объекты в Offer
                for data in js_offers:
                    try:
                        offer = Offer(
                            id=data.get('id', '0'),
                            lot_uuid=lot_uuid,
                            price=data.get('price', 0),
                            area=data.get('area', 0),
                            url=data.get('url', ''),
                            type=offer_type.split()[0],
                            address=data.get('address', '')
                        )
                        offers.append(offer)
                    except Exception as e:
                        log.warning(f"Ошибка при создании Offer из JS-данных: {str(e)}")
                return offers
            else:
                log.warning("JavaScript метод не вернул результатов")
    
        except Exception as e:
            log.exception(f"Ошибка при извлечении объявлений с {search_url}: {str(e)}")
        
        # Если прямая экстракция не удалась, вернем пустой список
        log.warning("Все методы извлечения данных не дали результатов")
        return []
    
    def unformatted_address_to_cian_search_filter(self, address):
        """Преобразует адрес в параметр поискового фильтра CIAN."""
        try:
            # Стандартизация адреса
            for old, new in address_replacements.items():
                address = address.replace(old, new)

            # Быстрые проверки для часто встречающихся случаев
            if "москва" in address.lower() and "область" not in address.lower():
                log.info("Быстро определен регион: Москва")
                return "region=1"  # ID Москвы
                
            if "область" in address.lower():
                log.info("Быстро определен регион: Московская область")
                return "region=4593"  # ID МО
                
            # Геокодинг адреса
            geocoding_response = self.get_json(CIAN_GEOCODE.format(address))
            
            # Находим подходящий элемент в ответе
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                if item.get("text", "").startswith("Россия, Моск"):
                    geocoding_result = item
                    break
                    
            if not geocoding_result:
                log.warning(f"Не найдено соответствие для адреса: {address}")
                return "region=4593" if "область" in address.lower() else "region=1"

            # Получаем координаты из результата геокодирования
            lon, lat = geocoding_result.get("coordinates", [0, 0])
                
            if "Московская область" in geocoding_result.get("text", ""):
                # Для Подмосковья ищем ближайший город/населенный пункт
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "locality"}
                )
                
                try:
                    # details levels structure: 0 - МО, 1 - округ/город, 2 - дальше
                    location_id = for_search_result['details'][1]['id']
                    log.info(f"Определена локация в МО с ID: {location_id}")
                    return f"location[0]={location_id}"
                except (KeyError, IndexError):
                    log.warning(f"Не удалось определить точную локацию в МО: {address}")
                    return "region=4593"  # Возвращаем всю МО

            else:  # "Москва"
                # Для Москвы пытаемся найти район
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "district"}
                )
                
                try:
                    # details levels structure: 0 - Москва, 1 - АО, 2 - "район Ховрино", 3 - дальше
                    district_name = (
                        for_search_result["details"][2]["fullName"]
                        .replace("район", "")
                        .replace("р-н", "")
                        .strip()
                    )

                    district_id = moscow_district_name_to_cian_id.get(district_name)
                    if district_id:
                        log.info(f"Определен район Москвы: {district_name} (ID: {district_id})")
                        return f"district[0]={district_id}"
                except (KeyError, IndexError):
                    pass

                # Если не удалось определить район, ищем по вхождению названия района в адрес
                for district_name, district_id in moscow_district_name_to_cian_id.items():
                    if district_name.lower() in address.lower():
                        log.info(f"Найден район в адресе: {district_name}")
                        return f"district[0]={district_id}"

                # Если и это не удалось, пробуем улицу
                try:
                    for_search_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "street"}
                    )
                    street_id = for_search_result['details'][-1]['id']
                    log.info(f"Определена улица с ID: {street_id}")
                    return f"street[0]={street_id}"
                except (KeyError, IndexError):
                    pass

                log.warning(f"Не удалось определить точное местоположение в Москве для: {address}")
                return "region=1"  # Вся Москва

        except Exception as e:
            log.exception(f"Ошибка при определении фильтра для адреса {address}: {e}")
            return "region=1"  # По умолчанию Москва
    
    def extract_offer_data(self, offer_url, offer_page, lot_uuid, offer_type):
        """Извлекает данные объявления из страницы"""
        try:
            # Ищем скрипт с данными объявления
            script_match = re.search(r'window\._cianConfig\[\'frontend-offer-card\'\]\.concat\((.*?)\);', offer_page)
            
            if not script_match:
                log.warning(f"Не найден скрипт с данными для {offer_url}")
                return None
                
            json_text = script_match.group(1)
            config = json.loads(json_text)
            
            state_block = next((block for block in config if "key" in block and block["key"] == "defaultState"), None)
            if not state_block:
                log.warning(f"Не найден state_block в данных для {offer_url}")
                return None
                
            state = state_block["value"]
            
            # Проверяем наличие данных объявления
            if "offerData" not in state or "offer" not in state["offerData"]:
                log.warning(f"Нет данных offerData в {offer_url}")
                return None
                
            offer_data = state["offerData"]["offer"]
            
            # Получаем площадь
            area = 0
            if "land" in offer_data:
                # Для земельных участков
                unit = offer_data["land"].get("areaUnitType", "")
                raw_area = float(offer_data["land"].get("area", 0))
                area = raw_area * (100 if unit == "sotka" else 10000 if unit == "hectare" else 1)
            else:
                # Для помещений
                area = float(offer_data.get("totalArea", 0))
            
            if area <= 0:
                log.warning(f"Некорректная площадь в {offer_url}")
                return None
            
            # Получаем цену
            price = 0
            if offer_type.startswith("sale"):
                price = offer_data.get("priceTotalRur", 0)
            else:
                price = offer_data.get("priceTotalPerMonthRur", 0)
                
            if price <= 0:
                log.warning(f"Некорректная цена в {offer_url}")
                return None
            
            # Получаем адрес
            address = ""
            try:
                if "adfoxOffer" in state:
                    address = state["adfoxOffer"]["response"]["data"]["unicomLinkParams"].get("puid14", "")
                
                if not address and "address" in offer_data:
                    address = offer_data.get("address", "")
            except:
                pass
            
            # Создаем объект объявления
            oid = str(offer_data.get("cianId", 0)) or offer_url.split('/')[-2]
            
            return {
                "id": oid,
                "lot_uuid": lot_uuid,
                "price": price,
                "area": area,
                "url": offer_url,
                "type": offer_type.split()[0],
                "address": address
            }
            
        except Exception as e:
            log.exception(f"Ошибка при извлечении данных объявления {offer_url}: {e}")
            return None
    
    def fetch_nearby_offers(self, search_filter, lot_uuid) -> Tuple[List[Offer], List[Offer]]:
        """Получает объявления о продаже и аренде по заданному фильтру"""
        log.info(f"Запрос предложений для фильтра: {search_filter}")
        
        sale_offers = []
        rent_offers = []
        
        # Добавляем случайные параметры для обхода кэширования
        random_params = f"&ad={random.randint(1000, 9999)}&ts={int(time.time())}"
        
        # URL-шаблоны для разных типов поиска
        url_templates = [
            (CIAN_SALE_SEARCH, "sale"),
            (CIAN_SALE_SEARCH_LAND, "sale land"),
            (CIAN_RENT_SEARCH, "rent"),
            (CIAN_RENT_SEARCH_LAND, "rent land")
        ]
        
        # Для отладки начнем с начала списка, не перемешивая
        # Иначе можно добавить random.shuffle(url_templates)
        
        for url_template, offer_type in url_templates:
            log.info(f"Обработка типа объявлений: {offer_type}")
            try:
                # Формируем URL поиска
                search_url = url_template.format(search_filter) + random_params
                log.info(f"Поиск {offer_type}: {search_url}")
                
                # Обновляем главную страницу
                self.refresh_main_page()
                
                # Получаем страницу с результатами поиска
                search_page = self.get_page(search_url)
                if not search_page:
                    log.warning(f"Не удалось получить страницу поиска {offer_type}")
                    continue
                
                # Сохраняем для отладки
                save_debug_info(search_url, search_page, self.driver, f"search_{offer_type}")
                
                # Проверяем наличие результатов
                if "по вашему запросу ничего не найдено" in search_page.lower():
                    log.info(f"Нет результатов для {offer_type}")
                    continue
                
                # Пытаемся извлечь объявления напрямую со страницы поиска
                extracted_offers = self.extract_offers_from_search_page(
                    search_page, search_url, lot_uuid, offer_type
                )
                
                # Исправление некорректных данных
                valid_extracted_offers = []
                for offer in extracted_offers:
                    if offer.area <= 0:
                        offer.area = 1  # Устанавливаем минимальное значение
                        log.warning(f"Исправлена нулевая площадь для объявления {offer.id}")
                    
                    if offer.price <= 0:
                        log.warning(f"Пропуск объявления с нулевой ценой: {offer.id}")
                        continue
                        
                    valid_extracted_offers.append(offer)
                
                if valid_extracted_offers:
                    log.info(f"Успешно извлечено {len(valid_extracted_offers)} объявлений {offer_type} со страницы поиска")
                    if offer_type.startswith("sale"):
                        sale_offers.extend(valid_extracted_offers)
                    else:
                        rent_offers.extend(valid_extracted_offers)
                    continue  # Переходим к следующему типу, если извлекли достаточно объявлений
                
                # Если не удалось извлечь напрямую, используем старый метод
                # Извлекаем ссылки на объявления
                offer_urls = []
                
                # Пытаемся найти ссылки через регулярное выражение
                offer_links = re.findall(r'href="(/offer/[^"]+)"', search_page)
                offer_urls.extend([f"https://www.cian.ru{link}" for link in offer_links])
                
                # Также ищем ссылки на продажу/аренду
                sale_rent_links = re.findall(r'href="(/sale/[^"]+)"', search_page) + re.findall(r'href="(/rent/[^"]+)"', search_page)
                offer_urls.extend([f"https://www.cian.ru{link}" for link in sale_rent_links])
                
                log.info(f"Найдено {len(offer_urls)} ссылок на объявления {offer_type}")
                
                # Ограничиваем количество объявлений для анализа
                offer_urls = offer_urls[:10]
                
                # Обрабатываем каждое объявление
                for offer_url in offer_urls:
                    try:
                        # Обновляем главную страницу каждые 3-4 объявления
                        if random.random() < 0.3:
                            self.refresh_main_page()
                        
                        # Получаем страницу объявления
                        offer_page = self.get_page(offer_url)
                        if not offer_page:
                            continue
                        
                        # Извлекаем данные объявления
                        offer_data = self.extract_offer_data(offer_url, offer_page, lot_uuid, offer_type)
                        if not offer_data:
                            continue
                        
                        # Создаем объект объявления и добавляем в соответствующий список
                        offer = Offer(**offer_data)
                        
                        if offer_type.startswith("sale"):
                            sale_offers.append(offer)
                        else:
                            rent_offers.append(offer)
                            
                    except Exception as e:
                        log.error(f"Ошибка при обработке объявления {offer_url}: {e}")
                        continue
                
            except Exception as e:
                log.error(f"Общая ошибка при обработке типа {offer_type}: {e}", exc_info=True)
                continue
        
        # Удаляем дубликаты
        unique_sale_offers = []
        unique_ids = set()
        for offer in sale_offers:
            if offer.id not in unique_ids:
                unique_ids.add(offer.id)
                unique_sale_offers.append(offer)
        
        unique_rent_offers = []
        unique_ids = set()
        for offer in rent_offers:
            if offer.id not in unique_ids:
                unique_ids.add(offer.id)
                unique_rent_offers.append(offer)
        
        log.info(f"Собрано {len(unique_sale_offers)} предложений о продаже и {len(unique_rent_offers)} предложений об аренде")
        return unique_sale_offers, unique_rent_offers
    
    def close(self):
        """Закрывает браузер"""
        if self.driver:
            self.driver.quit()


# Singleton-экземпляр парсера
_parser_instance = None

def get_parser():
    """Возвращает единственный экземпляр CianParser"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = CianParser()
    return _parser_instance

def fetch_nearby_offers(search_filter: str, lot_uuid) -> Tuple[List[Offer], List[Offer]]:
    """
    Внешний интерфейс для получения предложений с CIAN.
    Используется в main.py
    """
    parser = get_parser()
    return parser.fetch_nearby_offers(search_filter, lot_uuid)

def unformatted_address_to_cian_search_filter(address: str) -> str:
    """
    Внешний интерфейс для преобразования адреса в поисковый фильтр.
    Используется в main.py
    """
    parser = get_parser()
    return parser.unformatted_address_to_cian_search_filter(address)