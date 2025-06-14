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

def refresh_session(self):
    """Перезапускает сессию браузера для освобождения памяти"""
    try:
        if self.driver:
            current_cookies = self.driver.get_cookies()
            self.driver.quit()
            
        time.sleep(5)
        
        self.initialize_driver()
        
        if hasattr(self, 'driver') and self.driver and 'current_cookies' in locals():
            self.driver.get(CIAN_MAIN_URL)
            for cookie in current_cookies:
                try:
                    self.driver.add_cookie(cookie)
                except:
                    pass
                    
        log.info("Сессия браузера успешно перезапущена")
        return True
    except Exception as e:
        log.error(f"Ошибка при перезапуске сессии браузера: {e}")
        return False

def save_debug_info(url: str, html: str, driver=None, prefix="page"):
    """Сохраняет HTML страницы и скриншот для отладки."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_base = os.path.join(LOG_DIR, f"{prefix}_{timestamp}")
    
    html_filename = f"{filename_base}.html"
    with open(html_filename, "w", encoding="utf-8") as f:
        f.write(f"<!-- URL: {url} -->\n{html}")
    log.info(f"Сохранен HTML в {html_filename}")
    
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
        self._address_filter_cache = {}
        
    def initialize_driver(self):
        log.info("Инициализация драйвера Chrome...")
        
        try:
            if self.driver:
                self.driver.quit()
        except:
            pass
        
        options = uc.ChromeOptions()
        options.add_argument(f"--user-agent={UserAgent().random}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.page_load_strategy = "eager"
        
        self.driver = uc.Chrome(options=options, headless=False)
        self.driver.set_page_load_timeout(60)
        
        # Открываем главную страницу
        self.driver.get(CIAN_MAIN_URL)
        time.sleep(5)
        self.first_tab = self.driver.current_window_handle
        
        # Открываем новую вкладку
        self.driver.switch_to.new_window("tab")
        
        log.info("Драйвер Chrome успешно инициализирован")
    
        
    def refresh_main_page(self):
        """Обновляет главную страницу как в рабочей версии"""
        try:
            current_tab = self.driver.current_window_handle
            self.driver.switch_to.window(self.first_tab)
            self.driver.refresh()
            time.sleep(5)
            self.driver.switch_to.window(current_tab)
        except Exception:
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
        try:
            time.sleep(10)
            self.driver.get(url)
            return self.driver.page_source
        except Exception as e:
            log.warning(f"Ошибка при загрузке страницы {url}: {e}")
            # В точности как driver_setup() в рабочей версии - пересоздаем драйвер
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass
            self.initialize_driver()
            # Рекурсивно пытаемся еще раз
            return self.get_page(url)

    

    """ def get_page(self, url):
        try:
            # Запускаем загрузку страницы
            self.driver.get(url)
            
            # Увеличиваем таймаут загрузки
            self.driver.set_page_load_timeout(90)
            
            # Проверяем тип страницы для разного ожидания
            if "cat.php" in url:  # Это страница поиска
                # 1. Ожидаем загрузку основного контейнера результатов
                try:
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='SearchContainer'], ._32bbee5fda--serp--bTAO_"))
                    )
                    
                    # 2. Проверяем индикаторы загрузки и ждем их исчезновения
                    try:
                        loading_indicator = WebDriverWait(self.driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='Spinner'], .c6e8ba5398--loader--HqDVk"))
                        )
                        # Ждем исчезновения индикатора загрузки если он есть
                        WebDriverWait(self.driver, 30).until(
                            EC.staleness_of(loading_indicator)
                        )
                    except:
                        # Если индикатор не найден, возможно, страница уже загружена
                        pass
                    
                    # 3. Дополнительно ждем появления результатов или сообщения о пустом результате
                    WebDriverWait(self.driver, 10).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, 
                            "a[data-name='CommercialTitle'], div[data-name='EmptyMessage'], ._32bbee5fda--container--pBaJE")) > 0
                    )
                    
                    # 4. Для пустых результатов - дополнительная проверка
                    empty_results = self.driver.find_elements(By.CSS_SELECTOR, "div[data-name='EmptyMessage']")
                    if empty_results:
                        log.info("Обнаружено сообщение о пустых результатах")
                    else:
                        # Если есть результаты, проверим их количество
                        offers = self.driver.find_elements(By.CSS_SELECTOR, "a[data-name='CommercialTitle']")
                        log.info(f"Найдено {len(offers)} объявлений на странице")
                    
                    # 5. Небольшая дополнительная пауза для завершения рендеринга
                    time.sleep(2)
                    
                except Exception as wait_error:
                    log.warning(f"Ожидание элементов страницы поиска не завершилось: {wait_error}")
                    # Даем дополнительное время на прогрузку
                    time.sleep(10)
                    
            elif "commercial" in url:  # Это страница объявления
                try:
                    # Ждем загрузки ключевых элементов на странице объявления
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 
                            "[data-name='CommercialFullHeight'], [data-name='OfferTitle']"))
                    )
                    
                    # Короткая пауза для завершения загрузки скриптов
                    time.sleep(2)
                    
                except Exception as wait_error:
                    log.warning(f"Ожидание элементов страницы объявления не завершилось: {wait_error}")
                    time.sleep(5)
                    
            else:
                # Для других страниц просто ждем небольшую паузу
                time.sleep(5)
                
            # Проверка на captcha или блокировку
            if any(marker in self.driver.page_source.lower() for marker in ["captcha", "подтвердите, что вы не робот"]):
                log.warning("⚠️ Обнаружена CAPTCHA! Требуется вмешательство")
                # Сохраняем страницу и скриншот для анализа
                save_debug_info(url, self.driver.page_source, self.driver, "captcha")
                # Даем время на ручное решение
                time.sleep(15)
                
            return self.driver.page_source
            
        except Exception as e:
            log.warning(f"Ошибка при загрузке страницы {url}: {e}")
            
            # В точности как driver_setup() в рабочей версии - пересоздаем драйвер
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass
                
            self.initialize_driver()
            
            # Добавляем паузу перед повторной попыткой для уменьшения нагрузки
            time.sleep(5)
            
            # Рекурсивно пытаемся еще раз
            return self.get_page(url)"""
    
    def get_json(self, url, _retries=0):
        """Получает JSON как в рабочей версии"""
        try:
            self.driver.get(url)
        except Exception:
            self.initialize_driver()
            return self.get_json(url)

        if _retries > 0:
            time.sleep(10)

        try:
            return json.loads(self.driver.find_element(By.TAG_NAME, "pre").text)
        except Exception:
            if _retries < 10:
                time.sleep(1)
                return self.get_json(url, _retries + 1)
            raise

    def post_json(self, url, body, _retries=0):
        """Отправляет POST запрос как в рабочей версии"""
        try:
            self.driver.execute_script(
                """
                function post(path, params, method='post') {
                    const form = document.createElement('form');
                    form.method = method;
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
                """,
                body,
                url,
            )
        except Exception:
            self.initialize_driver()
            return self.post_json(url, body)

        if _retries > 0:
            time.sleep(5)

        try:
            return json.loads(self.driver.find_element(By.TAG_NAME, "pre").text)
        except Exception:
            if _retries < 10:
                time.sleep(1)
                return self.get_json(url, _retries + 1)
            raise
    
    def extract_offers_from_search_page(self, search_page, search_url, lot_uuid, offer_type):
        """Извлекает объявления с поддержкой разных форматов страницы"""
        offers = []
        
        # Проверка на пустые результаты
        if "по вашему запросу ничего не найдено" in search_page.lower():
            log.info(f"Нет результатов для {offer_type}")
            return []
        
        try:
            # Используем более мощный парсер lxml
            search_soup = BeautifulSoup(search_page, 'lxml')
            
            # Поиск по ВСЕМ возможным селекторам объявлений (в порядке приоритета)
            offer_links = search_soup.find_all("a", attrs={"data-name": "CommercialTitle"})
            
            # Если не нашли объявления с первым селектором, пробуем альтернативные
            if not offer_links:
                log.info("Основной селектор не сработал, использую альтернативные...")
                offer_links = search_soup.select('.c6e8ba5398--offer-container--pCGiP a[href*="/commercial/"]')
                
            # Если и это не сработало, ищем любые ссылки на коммерческие объекты
            if not offer_links:
                log.info("Используем общий селектор для коммерческой недвижимости...")
                offer_links = search_soup.select('a[href*="/commercial/"]')
                
            # Если не нашли ничего - пробуем искать в DOM любые ссылки на объекты коммерческой недвижимости
            if not offer_links:
                # Пробуем последний вариант - искать именно по содержимому href
                log.info("Ищем любые ссылки на коммерческую недвижимость...")
                offer_links = [link for link in search_soup.find_all('a') 
                            if link.has_attr('href') and '/commercial/' in link['href']]
            
            # Извлекаем URL и удаляем дубликаты
            offer_urls = []
            seen_urls = set()
            
            for link in offer_links:
                href = link.get('href')
                if href and "/commercial/" in href and href not in seen_urls:
                    if not href.startswith('http'):
                        href = f"https://www.cian.ru{href}"
                    offer_urls.append(href)
                    seen_urls.add(href)
            
            log.info(f"Найдено {len(offer_urls)} уникальных ссылок на объявления {offer_type}")
            
            # Сохраняем страницу поиска для отладки если нет ссылок
            if not offer_urls:
                save_debug_info(search_url, search_page, self.driver, f"no_links_{offer_type}")
                log.warning(f"Не найдено ссылок на объявления в странице поиска для {offer_type}!")
                return []
            
            # Ограничим количество обрабатываемых объявлений
            max_offers = 20
            if len(offer_urls) > max_offers:
                log.info(f"Ограничиваем до {max_offers} объявлений из {len(offer_urls)}")
                offer_urls = offer_urls[:max_offers]
            log.info(f"Найдено {len(offer_urls)} ссылок на объявления {offer_type}")
            
            # Обрабатываем каждое объявление
            for i, offer_url in enumerate(offer_urls):
                try:
                    log.info(f"Загрузка страницы объявления [{i+1}/{len(offer_urls)}]: {offer_url}")
                    
                    # Пауза между запросами
                    time.sleep(random.uniform(2, 4)) 
                    
                    # Получаем страницу объявления
                    offer_page = self.get_page(offer_url)
                    if not offer_page:
                        log.warning(f"Не удалось получить страницу объявления: {offer_url}")
                        continue
                    
                    # Обрабатываем страницу с помощью BeautifulSoup и извлекаем данные
                    offer_soup = BeautifulSoup(offer_page, features="lxml")

                    try:
                        script_tag = next(
                            tag
                            for tag in offer_soup.find_all("script")
                            if "window._cianConfig['frontend-offer-card']" in tag.text
                        )
                    except StopIteration:
                        log.warning(f"Не найден скрипт с данными объявления в {offer_url}")
                        continue

                    config_json_string = (
                        script_tag.text.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
                    )
                    config_json = json.loads(config_json_string)
                    offer_info = next(
                        filter(lambda block: block["key"] == "defaultState", config_json)
                    )["value"]

                    # Извлекаем площадь в точности как в parsing_torgi_and_cian.py
                    area = None
                    try:
                        if "land" in offer_info["offerData"]["offer"]:
                            match offer_info["offerData"]["offer"]["land"]["areaUnitType"]:
                                case "sotka":
                                    area = float(offer_info["offerData"]["offer"]["land"]["area"]) * 100
                                case "hectare":
                                    area = float(offer_info["offerData"]["offer"]["land"]["area"]) * 10000
                    except (LookupError, ValueError):
                        pass

                    if area is None:
                        area = float(offer_info["offerData"]["offer"].get("totalArea", 0))

                    if not area:
                        log.warning(f"Не найдена площадь для {offer_url}")
                        continue
                    
                    # Извлекаем данные объявления точно так же, как в parsing_torgi_and_cian.py
                    try:
                        # Извлекаем адрес из adfoxOffer
                        address = offer_info["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                        
                        # Определяем тип цены в зависимости от типа предложения
                        price = offer_info["offerData"]["offer"].get(
                            "priceTotalRur",
                            offer_info["offerData"]["offer"].get(
                                "priceTotalPerMonthRur",
                                0,
                            ),
                        )
                        
                        # Создаем объект предложения
                        offer = Offer(
                            id=offer_url.split('/')[-2],
                            lot_uuid=lot_uuid,
                            address=f"{address}",
                            area=area,
                            price=price,
                            url=offer_url,
                            type=offer_type.split()[0]
                        )
                        offers.append(offer)
                        log.info(f"✓ Успешно обработано объявление {offer_url}")
                        
                    except Exception as e:
                        log.error(f"Что-то не так с объявлением '{offer_url}': {e}")
                        continue
                    
                except Exception as e:
                    log.error(f"Ошибка при обработке объявления {offer_url}: {str(e)}")
                    continue
            
        except Exception as e:
            log.exception(f"Ошибка при извлечении объявлений с {search_url}: {str(e)}")
        
        return offers
    
    def unformatted_address_to_cian_search_filter(self, address: str) -> str:
        """
        Преобразует адрес в параметр поискового фильтра ЦИАН.
        
        Алгоритм:
        1. Сначала проверяем кэш, чтобы не делать лишних запросов
        2. Выполняем стандартизацию адреса (замена сокращений и т.д.)
        3. Проверяем простые случаи (явно указана Москва/МО)
        4. Выполняем геокодирование адреса через API ЦИАН
        5. Для МО ищем конкретный населенный пункт
        6. Для Москвы ищем сначала район
        7. Если район не найден - ищем улицу
        8. В случае неудачи возвращаемся на уровень региона
        
        Args:
            address: Текстовый адрес объекта
            
        Returns:
            Строка параметра для поискового URL ЦИАН
        """
        # Кэширование результатов для повторяющихся адресов
        if hasattr(self, '_address_filter_cache') and address in self._address_filter_cache:
            log.info(f"Кэш: Использован сохраненный фильтр для адреса «{address}»")
            return self._address_filter_cache[address]
            
        log.info(f"Определение поискового фильтра для адреса: «{address}»")
        
        # Нормализация адреса
        normalized_address = address
        for old, new in address_replacements.items():
            normalized_address = normalized_address.replace(old, new)
        
        # Базовые быстрые проверки по ключевым словам
        address_lower = normalized_address.lower()
        
        # Явно указан район Москвы - прямое соответствие
        for district_name, district_id in moscow_district_name_to_cian_id.items():
            district_pattern = f"район {district_name.lower()}"
            if district_pattern in address_lower or f"{district_name.lower()} район" in address_lower:
                result = f"district[0]={district_id}"
                log.info(f"Быстрое определение: найден район «{district_name}» (ID: {district_id})")
                
                # Сохраняем в кэш
                if hasattr(self, '_address_filter_cache'):
                    self._address_filter_cache[address] = result
                return result
        
        # Подготовка результата по умолчанию
        default_result = None
        
        # Явная проверка на Московскую область и Москву
        if "москва" in address_lower and "область" not in address_lower:
            default_result = "region=1"
            log.info("Базовое определение региона: Москва")
        elif "область" in address_lower or "мо" in re.findall(r'\bмо\b', address_lower):
            default_result = "region=4593" 
            log.info("Базовое определение региона: Московская область")
        
        try:
            # Геокодирование через API ЦИАН
            geocoding_response = self.get_json(CIAN_GEOCODE.format(normalized_address))
            
            # Поиск подходящего результата геокодирования
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                item_text = item.get("text", "")
                if item_text.startswith("Россия, Моск"):  # Подходят и Москва, и Московская область
                    geocoding_result = item
                    log.info(f"Результат геокодирования: {item_text}")
                    break
                    
            if not geocoding_result:
                log.warning(f"Не найдено геокодирование для адреса: {address}")
                return default_result or ("region=4593" if "область" in address_lower else "region=1")

            # Получаем координаты
            lon, lat = geocoding_result.get("coordinates", [0, 0])
            if lon == 0 or lat == 0:
                log.warning("Получены нулевые координаты, используем регион по умолчанию")
                return default_result or ("region=4593" if "область" in address_lower else "region=1")
                
            # Логика для Московской области
            if "Московская область" in geocoding_result.get("text", ""):
                log.info("Определен регион: Московская область")
                
                try:
                    # Определяем конкретный населенный пункт в МО
                    for_search_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "locality"}
                    )
                    
                    # Проверяем структуру ответа
                    if for_search_result and "details" in for_search_result and len(for_search_result["details"]) > 1:
                        location_id = for_search_result['details'][1]['id']
                        location_name = for_search_result['details'][1].get('fullName', 'Неизвестно')
                        log.info(f"Определена локация в МО: {location_name} (ID: {location_id})")
                        result = f"location[0]={location_id}"
                        
                        # Сохраняем в кэш
                        if hasattr(self, '_address_filter_cache'):
                            self._address_filter_cache[address] = result
                        return result
                    else:
                        log.warning(f"Некорректная структура ответа для населенного пункта в МО")
                except (KeyError, IndexError, Exception) as e:
                    log.warning(f"Ошибка при определении локации в МО: {e}")
                
                # Если не удалось определить конкретный населенный пункт
                return "region=4593"
                
            # Логика для Москвы
            else:
                log.info("Определен регион: Москва")
                
                try:
                    # Пытаемся определить район в Москве
                    for_search_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "district"}
                    )
                    
                    # Проверка наличия района в ответе
                    if (for_search_result and "details" in for_search_result and 
                        len(for_search_result["details"]) > 2 and 
                        "fullName" in for_search_result["details"][2]):
                        
                        # Извлекаем имя района
                        district_name = (
                            for_search_result["details"][2]["fullName"]
                            .replace("район", "")
                            .replace("р-н", "")
                            .strip()
                        )

                        # Ищем ID района в справочнике
                        district_id = moscow_district_name_to_cian_id.get(district_name)
                        if district_id:
                            log.info(f"Определен район Москвы: {district_name} (ID: {district_id})")
                            result = f"district[0]={district_id}"
                            
                            # Сохраняем в кэш
                            if hasattr(self, '_address_filter_cache'):
                                self._address_filter_cache[address] = result
                            return result
                        else:
                            log.warning(f"Район '{district_name}' не найден в справочнике")
                    else:
                        log.info("Район не найден в структуре ответа API")
                        
                except (KeyError, IndexError, Exception) as e:
                    log.warning(f"Ошибка при определении района: {e}")
                
                # План Б: Ищем упоминание любого района в адресе
                for district_name, district_id in moscow_district_name_to_cian_id.items():
                    if district_name.lower() in address_lower:
                        log.info(f"Найден район в адресе: {district_name}")
                        result = f"district[0]={district_id}"
                        
                        # Сохраняем в кэш
                        if hasattr(self, '_address_filter_cache'):
                            self._address_filter_cache[address] = result
                        return result
                
                # План В: Используем расширенный нечеткий поиск
                def key_function(pair):
                    # Если район найден в адресе, вернем его позицию, иначе - длину адреса (= не найден)
                    return index if (index := address_lower.find(pair[0].lower())) != -1 else len(address_lower)
                
                try:
                    # Находим район, который с наибольшей вероятностью соответствует адресу
                    found_district_name_and_id = min(
                        moscow_district_name_to_cian_id.items(),
                        key=key_function
                    )
                    
                    # Если действительно нашли соответствие
                    if key_function(found_district_name_and_id) < len(address_lower):
                        district_name, district_id = found_district_name_and_id
                        log.info(f"Нечеткое соответствие района: {district_name}")
                        result = f"district[0]={district_id}"
                        
                        # Сохраняем в кэш
                        if hasattr(self, '_address_filter_cache'):
                            self._address_filter_cache[address] = result
                        return result
                except Exception as e:
                    log.warning(f"Ошибка при нечетком поиске района: {e}")
                
                # План Г: Если район не определили - пробуем улицу
                try:
                    for_search_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "street"}
                    )
                    
                    if "details" in for_search_result and len(for_search_result["details"]) > 0:
                        street_id = for_search_result['details'][-1]['id']
                        street_name = for_search_result['details'][-1].get('fullName', 'Неизвестно')
                        log.info(f"Определена улица: {street_name} (ID: {street_id})")
                        result = f"street[0]={street_id}"
                        
                        # Сохраняем в кэш
                        if hasattr(self, '_address_filter_cache'):
                            self._address_filter_cache[address] = result
                        return result
                    else:
                        log.warning("Структура ответа для улицы некорректна")
                except (KeyError, IndexError, Exception) as e:
                    log.warning(f"Ошибка при определении улицы: {e}")
            
        except Exception as e:
            log.exception(f"❌ Общая ошибка при определении фильтра для адреса {address}: {e}")
        
        # Если все методы не сработали, используем общий фильтр по региону
        final_region = "region=4593" if "область" in address_lower else "region=1"
        log.warning(f"⚠️ Не удалось определить точное местоположение для: {address}, используем {final_region}")
        
        # Сохраняем даже дефолтное значение в кэш, чтобы не повторять ошибку
        if hasattr(self, '_address_filter_cache'):
            self._address_filter_cache[address] = final_region
        
        return final_region
    
    def extract_offer_data(self, offer_url, offer_page, lot_uuid, offer_type):
        """Извлекает данные с отдельной страницы объявления в стиле parsing_torgi_and_cian.py"""
        try:
            # Извлекаем ID объявления для логов
            offer_id = offer_url.split('/')[-2] if offer_url.endswith('/') else offer_url.split('/')[-1]
            log.info(f"Извлечение данных объявления {offer_id}")
            
            # Сохраняем страницу для отладки
            debug_file = os.path.join(LOG_DIR, f"detail_page_{offer_id}_{int(time.time())}.html")
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(f"<!-- URL: {offer_url} -->\n{offer_page}")
            
            # ВАЖНО: Используем BeautifulSoup как в parsing_torgi_and_cian.py
            offer_soup = BeautifulSoup(offer_page, features="lxml")

            try:
                script_tag = next(
                    tag
                    for tag in offer_soup.find_all("script")
                    if "window._cianConfig['frontend-offer-card']" in tag.text
                )
            except StopIteration:
                log.warning(f"Не найден скрипт с данными объявления в {offer_url}")
                return None

            config_json_string = (
                script_tag.text.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
            )
            config_json = json.loads(config_json_string)
            offer_info = next(
                filter(lambda block: block["key"] == "defaultState", config_json)
            )["value"]
            
            # Извлекаем данные предложения
            if "offerData" not in offer_info or "offer" not in offer_info["offerData"]:
                log.warning(f"Не найдены данные объявления в {offer_url}")
                return None
                    
            offer_data = offer_info["offerData"]["offer"]
            
            # Извлекаем площадь
            area = 0
            if "land" in offer_data:
                unit = offer_data["land"].get("areaUnitType", "")
                raw_area = float(offer_data["land"].get("area", 0))
                area = raw_area * (100 if unit == "sotka" else 10000 if unit == "hectare" else 1)
            else:
                area = float(offer_data.get("totalArea", 0))
            
            if area <= 0:
                log.warning(f"Некорректная площадь в {offer_url}")
                return None
            
            # Извлекаем цену
            price = 0
            if offer_type.startswith("sale"):
                price = offer_data.get("priceTotalRur", 0)
            else:
                price = offer_data.get("priceTotalPerMonthRur", 0)
                    
            if price <= 0:
                log.warning(f"Некорректная цена в {offer_url}")
                return None
            
            # Извлекаем адрес ТОЧНО КАК в parsing_torgi_and_cian.py
            try:
                address = offer_info["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                log.info(f"📍 Получен адрес из adfoxOffer: {address}")
            except (KeyError, TypeError):
                log.warning(f"❌ Не удалось получить адрес через стандартный путь для {offer_url}")
                address = "Москва"  # Дефолтное значение
            
            # Префикс источника
                
            log.info(f"📍 Сохраняем адрес объявления {offer_id}: '{address}'")
                
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
            log.exception(f"❌ Ошибка при извлечении данных объявления {offer_url}: {e}")
            return None
    
    def fetch_nearby_offers(self, search_filter, lot_uuid):
        """Получает объявления о продаже и аренде с корректными паузами"""
        log.info(f"Запрос предложений для фильтра: {search_filter}")
        
        sale_offers = []
        rent_offers = []
        
        # Обновим порядок запросов и добавим адекватные паузы
        search_types = [
            (sale_offers, "sale", CIAN_SALE_SEARCH, "sale offers", 8),  # sale имеет приоритет
            (rent_offers, "rent", CIAN_RENT_SEARCH, "rent offers", 7),
            (sale_offers, "sale land", CIAN_SALE_SEARCH_LAND, "sale offers (land)", 6),
            (rent_offers, "rent land", CIAN_RENT_SEARCH_LAND, "rent offers (land)", 5),
        ]
        # Точно как в parsing_torgi_and_cian.py
        for offer_list, offer_type, url_template, tqdm_desc in [
            (sale_offers, "sale", CIAN_SALE_SEARCH, "sale offers"),
            (sale_offers, "sale land", CIAN_SALE_SEARCH_LAND, "sale offers (land)"),
            (rent_offers, "rent", CIAN_RENT_SEARCH, "rent offers"),
            (rent_offers, "rent land", CIAN_RENT_SEARCH_LAND, "rent offers (land)"),
        ]:
            log.info(f"Обработка типа объявлений: {offer_type}")
            try:
                self.driver.delete_all_cookies()
                self.refresh_main_page()
                
                # Серьезная пауза перед важным запросом sale
                if offer_type == "sale":
                    time.sleep(10)
                else:
                    time.sleep(10)
                # Формируем URL поиска
                search_url = url_template.format(search_filter)
                log.info(f"Поиск {offer_type}: {search_url}")
                
                # Обновляем главную страницу для сброса состояния
                self.refresh_main_page()
                
                # Получаем страницу поиска
                search_page = self.get_page(search_url)
                if not search_page:
                    log.warning(f"Не удалось получить страницу поиска {offer_type}")
                    continue
                
                # Сохраняем страницу для отладки
                save_debug_info(search_url, search_page, self.driver, f"search_{offer_type}")
                
                # Проверяем наличие результатов
                if "по вашему запросу ничего не найдено" in search_page.lower():
                    log.info(f"Нет результатов для {offer_type}")
                    continue
                
                # Получаем объявления
                extracted_offers = self.extract_offers_from_search_page(
                    search_page, search_url, lot_uuid, offer_type
                )
                
                if offer_type.startswith("sale"):
                    sale_offers.extend(extracted_offers)
                else:
                    rent_offers.extend(extracted_offers)
                    
            except Exception as e:
                log.error(f"Ошибка при обработке типа {offer_type}: {e}")
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