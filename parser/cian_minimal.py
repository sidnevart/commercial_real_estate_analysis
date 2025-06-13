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
            
        time.sleep(3)
        
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
        
    def initialize_driver(self):
        """Инициализирует драйвер Chrome с расширенными возможностями обхода обнаружения"""
        log.info("Инициализация обновленного драйвера Chrome...")
        
        options = uc.ChromeOptions()
        
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ]
        user_agent = random.choice(user_agents)
        options.add_argument(f"--user-agent={user_agent}")
        log.info(f"Используем User-Agent: {user_agent}")
        
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        width = random.randint(1200, 1600)
        height = random.randint(800, 1000)
        options.add_argument(f"--window-size={width},{height}")
        
        options.page_load_strategy = "eager"
        
        if random.random() < 0.5:
            options.add_argument("--incognito")
        
        try:
            self.driver = uc.Chrome(options=options, headless=False)
            self.driver.set_page_load_timeout(60)
            
            self.driver.execute_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
            
            // Удаляем скрипты автоматизации
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            """)
            
            popular_sites = ["https://www.google.ru", "https://www.yandex.ru"]
            random.shuffle(popular_sites)
            
            for site in popular_sites[:1]:
                self.driver.get(site)
                time.sleep(random.uniform(2.0, 4.0))
                
                self.driver.execute_script(f"window.scrollBy(0, {random.randint(300, 800)})")
                time.sleep(random.uniform(1.0, 2.0))
            
            self.driver.get(CIAN_MAIN_URL)
            time.sleep(random.uniform(3.0, 5.0))
            self.first_tab = self.driver.current_window_handle
            
            common_cookies = [
                {"name": "visited_before", "value": "true", "domain": ".cian.ru"},
                {"name": "session_region_id", "value": "1", "domain": ".cian.ru"},
                {"name": "login_mro_popup", "value": "1", "domain": ".cian.ru"},
                {"name": "_ga", "value": f"GA1.2.{random.randint(1000000, 9999999)}.{int(time.time()-random.randint(10000, 90000))}", "domain": ".cian.ru"}
            ]
            
            for cookie in common_cookies:
                try:
                    self.driver.add_cookie(cookie)
                except:
                    pass
            
            self.driver.execute_script(f"window.scrollBy(0, {random.randint(100, 300)})")
            time.sleep(random.uniform(0.5, 1.5))
            self.driver.execute_script(f"window.scrollBy(0, {random.randint(300, 600)})")
            time.sleep(random.uniform(0.5, 1.0))
            self.driver.execute_script(f"window.scrollBy(0, {random.randint(-200, -100)})")
            time.sleep(random.uniform(0.3, 0.7))
                
            self.driver.switch_to.new_window("tab")
            log.info("Драйвер Chrome успешно инициализирован с улучшенной защитой от обнаружения")
            
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
        """Получает страницу с повторными попытками и обходом защиты"""
        max_attempts = 4
        for attempt in range(max_attempts):
            try:
                log.info(f"Загрузка страницы {url} (попытка {attempt+1}/{max_attempts})")
                self.driver.get(url)
                
                time.sleep(random.uniform(3, 5))
                
                self.driver.execute_script("""
                var event = document.createEvent('MouseEvents');
                event.initMouseEvent('mousemove', true, true, window, 1, 
                    Math.floor(Math.random() * window.innerWidth), 
                    Math.floor(Math.random() * window.innerHeight), 
                    Math.floor(Math.random() * window.innerWidth), 
                    Math.floor(Math.random() * window.innerHeight), 
                    false, false, false, false, 0, null);
                document.dispatchEvent(event);
                """)
                
                scroll_iterations = random.randint(2, 5)
                for _ in range(scroll_iterations):
                    scroll_amount = random.randint(100, 500)
                    self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                    time.sleep(random.uniform(0.7, 1.5))
                
                if random.random() < 0.4:
                    self.driver.execute_script(f"window.scrollBy(0, {random.randint(-300, -100)})")
                    time.sleep(random.uniform(0.5, 1.0))
                
                try:
                    self.driver.execute_script("""
                    function isElementInViewport(el) {
                        var rect = el.getBoundingClientRect();
                        return (
                            rect.top >= 0 &&
                            rect.left >= 0 &&
                            rect.bottom <= (window.innerHeight || document.documentElement.clientHeight) &&
                            rect.right <= (window.innerWidth || document.documentElement.clientWidth)
                        );
                    }
                    
                    // Находим все изображения с отложенной загрузкой
                    var lazyImages = Array.from(document.querySelectorAll('img[data-src], img[data-original]'));
                    
                    // Проверяем, есть ли они в поле зрения, и загружаем, если да
                    lazyImages.forEach(function(img) {
                        if (isElementInViewport(img)) {
                            if (img.dataset.src) img.src = img.dataset.src;
                            if (img.dataset.original) img.src = img.dataset.original;
                        }
                    });
                    """)
                except Exception:
                    pass
                
                html = self.driver.page_source
                
                blocking_markers = [
                    "временно недоступна", 
                    "captcha", 
                    "подтвердите что вы не робот",
                    "обнаружили подозрительную активность",
                    "доступ ограничен",
                    "слишком много запросов"
                ]
                if any(marker in html.lower() for marker in blocking_markers):
                    log.warning(f"Обнаружены признаки блокировки на попытке {attempt+1}, обновляем сессию")
                    
                    block_page_file = os.path.join(LOG_DIR, f"block_page_{int(time.time())}.html")
                    with open(block_page_file, "w", encoding="utf-8") as f:
                        f.write(f"<!-- URL: {url} -->\n{html}")
                    
                    screenshot_file = os.path.join(LOG_DIR, f"block_screen_{int(time.time())}.png")
                    try:
                        self.driver.save_screenshot(screenshot_file)
                    except:
                        pass
                        
                    self.refresh_main_page()
                    
                    time.sleep(random.uniform(10, 15)) 
                    continue
                
                cookies = self.driver.get_cookies()
                if len(cookies) > 3: 
                    cookies_file = os.path.join(LOG_DIR, "working_cookies.json")
                    try:
                        with open(cookies_file, "w") as f:
                            json.dump(cookies, f)
                    except Exception as e:
                        log.debug(f"Не удалось сохранить куки: {e}")
                    
                return html
                
            except Exception as e:
                log.error(f"Ошибка при получении страницы {url}: {e}")
                self.refresh_main_page()
                time.sleep(random.uniform(5, 10)) 
        
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
            log.info(f"Анализ страницы поиска для {offer_type}: {search_url}")
            
            debug_file = os.path.join(LOG_DIR, f"search_page_{int(time.time())}.html")
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(search_page)
            log.info(f"Сохранена страница поиска для отладки: {debug_file}")
            
            # Метод 1: Извлечение из window.ca("pageview", {...})
            match = re.search(r'window\.ca\("pageview",(\{.*?\})\)', search_page)
            if match:
                try:
                    json_str = match.group(1)
                    data = json.loads(json_str)
                    if 'products' in data and isinstance(data['products'], list):
                        for product in data['products']:
                            if 'id' not in product:
                                continue
                                
                            oid = str(product.get('id', ''))
                            if not oid or not isinstance(oid, str) or not oid.isdigit():
                                log.warning(f"Некорректный ID '{oid}', генерируем новый")
                                oid = str(int(time.time() * 1000) + random.randint(1000, 9999))
                                
                            price = 0
                            if offer_type.startswith("sale"):
                                price_candidates = [
                                    product.get('price', 0),
                                    product.get('price', {}).get('value', 0) if isinstance(product.get('price'), dict) else 0
                                ]
                                price = next((p for p in price_candidates if p), 0)
                            else:
                                price_candidates = [
                                    product.get('price', 0),
                                    product.get('price', {}).get('value', 0) if isinstance(product.get('price'), dict) else 0
                                ]
                                price = next((p for p in price_candidates if p), 0)
                            
                            area = 0
                            area_str = ""
                            if 'features' in product and isinstance(product['features'], dict):
                                area_str = product['features'].get('area', '')
                            elif 'features' in product and isinstance(product['features'], list):
                                for feature in product['features']:
                                    if isinstance(feature, dict) and 'name' in feature and 'м²' in feature.get('name', ''):
                                        area_str = feature.get('value', '')
                                        break
                            
                            if not area_str and 'headline' in product and 'м²' in product['headline']:
                                area_str = product['headline']
                            
                            if area_str:
                                area_match = re.search(r'(\d+[.,]?\d*)\s*[мm]²', area_str.replace(' ', ''))
                                if area_match:
                                    area = float(area_match.group(1).replace(',', '.'))
                            
                            address = ""
                            if 'geo' in product and isinstance(product['geo'], dict):
                                address = product['geo'].get('address', '')
                            elif 'address' in product:
                                address = product['address']
                                
                            if not address:
                                log.warning(f"Не найден адрес для объявления ID {oid}, пробуем альтернативные методы")
                                
                                metro = ""
                                if 'geo' in product and 'undergrounds' in product['geo'] and product['geo']['undergrounds']:
                                    metros = product['geo']['undergrounds']
                                    if isinstance(metros, list) and metros and len(metros) > 0:
                                        if isinstance(metros[0], dict) and 'name' in metros[0]:
                                            metro = metros[0]['name']
                                        elif isinstance(metros[0], str):
                                            metro = metros[0]
                                
                                district = ""
                                if 'geo' in product:
                                    if 'districtsInfo' in product['geo'] and product['geo']['districtsInfo']:
                                        districts = product['geo']['districtsInfo']
                                        if isinstance(districts, list) and districts and len(districts) > 0:
                                            if isinstance(districts[0], dict) and 'name' in districts[0]:
                                                district = districts[0]['name']
                                            elif isinstance(districts[0], str):
                                                district = districts[0]
                                    elif 'district' in product['geo']:
                                        district = product['geo']['district']
                                
                                location = ""
                                if 'geo' in product and 'locationName' in product['geo']:
                                    location = product['geo']['locationName']
                                
                                building_address = ""
                                if 'building' in product and 'address' in product['building']:
                                    building_address = product['building']['address']
                                
                                address_parts = []
                                if location:
                                    address_parts.append(f"Москва, {location}")
                                elif district:
                                    address_parts.append(f"Москва, район {district}")
                                else:
                                    address_parts.append("Москва")
                                    
                                if district and not location:
                                    address_parts.append(f"район {district}")
                                    
                                if metro:
                                    address_parts.append(f"м. {metro}")
                                    
                                if building_address:
                                    address_parts.append(building_address)
                                
                                if address_parts:
                                    address = ", ".join(address_parts)
                                    log.info(f"Сформирован альтернативный адрес для {oid}: {address}")
                            
                            if price > 0 and area > 0:
                                offer = Offer(
                                    id=oid,
                                    lot_uuid=lot_uuid,
                                    price=price,
                                    area=area,
                                    url=f"https://www.cian.ru/{offer_type.split()[0]}/commercial/{oid}/",
                                    type=offer_type.split()[0],
                                    address=address
                                )
                                offers.append(offer)
                        
                        log.info(f"Извлечено {len(offers)} объявлений из JSON-данных")
                        return offers
                except Exception as e:
                    log.warning(f"Ошибка при декодировании JSON-данных из скрипта: {str(e)}")
            
            # Метод 2: Извлечение из _CIAN_COMPONENT_DATA_
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
                                
                                if not oid or not isinstance(oid, str) or not oid.isdigit():
                                    log.warning(f"Некорректный ID '{oid}', генерируем новый")
                                    oid = str(int(time.time() * 1000) + random.randint(1000, 9999))
                                
                                if offer_type.startswith("sale"):
                                    href = f"https://www.cian.ru/sale/commercial/{oid}/"
                                else:
                                    href = f"https://www.cian.ru/rent/commercial/{oid}/"
                                
                                price = 0
                                price_candidates = [
                                    offer_data.get("bargainTerms", {}).get("priceRur", 0),
                                    offer_data.get("price", 0),
                                    offer_data.get("priceTotalRur", 0),
                                    offer_data.get("priceTotalPerMonthRur", 0)
                                ]
                                price = next((p for p in price_candidates if p), 0)
                                
                                area = 0
                                if "totalArea" in offer_data:
                                    area = float(offer_data["totalArea"])
                                elif "areaDetails" in offer_data and "totalArea" in offer_data["areaDetails"]:
                                    area = float(offer_data["areaDetails"]["totalArea"])
                                elif "area" in offer_data:
                                    area = float(offer_data["area"])
                                
                                address = ""
                                if "geo" in offer_data and "address" in offer_data["geo"]:
                                    address = offer_data["geo"]["address"]
                                elif "address" in offer_data:
                                    address = offer_data["address"]
                                
                                if not address:
                                    log.warning(f"Не найден адрес для объявления {oid}, пробуем альтернативные методы")
                                    
                                    metro = ""
                                    if "geo" in offer_data and "undergrounds" in offer_data["geo"]:
                                        undergrounds = offer_data["geo"]["undergrounds"]
                                        if undergrounds and isinstance(undergrounds, list) and len(undergrounds) > 0:
                                            if isinstance(undergrounds[0], dict) and "name" in undergrounds[0]:
                                                metro = undergrounds[0]["name"]
                                            elif isinstance(undergrounds[0], str):
                                                metro = undergrounds[0]

                                    # Извлечение района
                                    district = ""
                                    if "geo" in offer_data:
                                        if "districts" in offer_data["geo"]:
                                            districts = offer_data["geo"]["districts"]
                                            if districts and isinstance(districts, list) and len(districts) > 0:
                                                if isinstance(districts[0], dict) and 'name' in districts[0]:
                                                    district = districts[0]['name']
                                                elif isinstance(districts[0], str):
                                                    district = districts[0]
                                        elif "district" in offer_data["geo"]:
                                            district = offer_data["geo"]["district"]

                                    location = ""
                                    if "geo" in offer_data and "locationName" in offer_data["geo"]:
                                        location = offer_data["geo"]["locationName"]
                                    
                                    # Проверяем альтернативные пути к адресу
                                    if "fullAddress" in offer_data:
                                        address = offer_data["fullAddress"]
                                        log.info(f"Найден адрес через fullAddress: {address}")
                                    elif "location" in offer_data and "address" in offer_data["location"]:
                                        address = offer_data["location"]["address"]
                                        log.info(f"Найден адрес через location.address: {address}")
                                    elif "building" in offer_data and "address" in offer_data["building"]:
                                        address = offer_data["building"]["address"]
                                        log.info(f"Найден адрес через building.address: {address}")
                                    else:
                                        # Формируем адрес из компонентов
                                        address_parts = []
                                        
                                        if location:
                                            address_parts.append(f"Москва, {location}")
                                        elif district:
                                            address_parts.append(f"Москва, район {district}")
                                        else:
                                            address_parts.append("Москва")
                                        
                                        if metro:
                                            address_parts.append(f"м. {metro}")
                                        
                                        if address_parts:
                                            address = ", ".join(address_parts)
                                            log.info(f"Сформирован адрес из местоположения: {address}")
                                        else:
                                            address = "Москва" 
                                            log.warning(f"Установлен базовый адрес для {oid}: {address}")
                                
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
            
            # Метод 3: JavaScript-извлечение
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
                
                // Извлечение адреса или местоположения
                function extractLocation(card) {
                    // Приоритет - найти полный оригинальный адрес
                    const fullAddressSelectors = [
                        '[data-name="GeoLabel"]',
                        '[data-name="AddressContainer"]', 
                        '.address--GbMIh',
                        '.geo-address--U0Sxb',
                        '.address',
                        '.location-address'
                    ];
                    
                    // Сначала ищем полный адрес по основным селекторам
                    for (const selector of fullAddressSelectors) {
                        const addressElement = card.querySelector(selector);
                        if (addressElement && addressElement.textContent.trim().length > 10) {
                            console.log("Found full address:", addressElement.textContent.trim());
                            return addressElement.textContent.trim();
                        }
                    }
                    
                    // Если не нашли полный адрес - ищем метро, район и другие компоненты
                    const metroElement = card.querySelector('[data-name="MetroInfo"], [data-mark="Underground"], .underground-item, .geo-undergrounds-item--TWnYV, .underground-name');
                    const districtElement = card.querySelector('[data-name="DistrictInfo"], .district-name, .geo-location');
                    
                    // Формируем адрес из компонентов
                    const parts = [];
                    parts.push('Москва');
                    
                    if (districtElement) {
                        const districtText = districtElement.textContent.trim();
                        if (!districtText.toLowerCase().includes('район')) {
                            parts.push('район ' + districtText);
                        } else {
                            parts.push(districtText);
                        }
                    }
                    
                    if (metroElement) {
                        const metroText = metroElement.textContent.trim();
                        if (!metroText.toLowerCase().includes('м.')) {
                            parts.push('м. ' + metroText);
                        } else {
                            parts.push(metroText);
                        }
                    }
                    
                    return parts.join(', ');
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
                        
                        // Используем функцию извлечения адреса
                        const address = extractLocation(card);
                        
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
                
                for data in js_offers:
                    try:
                        offer_id = data.get('id', '0')
                        if not offer_id or not isinstance(offer_id, str) or not offer_id.isdigit():
                            log.warning(f"Некорректный ID '{offer_id}', генерируем новый")
                            offer_id = str(int(time.time() * 1000) + random.randint(1000, 9999))
                        
                        url = data.get('url', '')
                        if not url and offer_id:
                            if offer_type.startswith("sale"):
                                url = f"https://www.cian.ru/sale/commercial/{offer_id}/"
                            else:
                                url = f"https://www.cian.ru/rent/commercial/{offer_id}/"
                        
                        offer = Offer(
                            id=offer_id,
                            lot_uuid=lot_uuid,
                            price=data.get('price', 0),
                            area=data.get('area', 0),
                            url=url,
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
        
        try:
            screenshot_file = os.path.join(LOG_DIR, f"failed_search_{int(time.time())}.png")
            self.driver.save_screenshot(screenshot_file)
            log.warning(f"Сохранен скриншот при неудачном поиске: {screenshot_file}")
        except:
            pass
        
        log.warning("Все методы извлечения данных не дали результатов")
        return []
    
    def unformatted_address_to_cian_search_filter(self, address):
        """Преобразует адрес в параметр поискового фильтра CIAN."""
        try:
            for old, new in address_replacements.items():
                address = address.replace(old, new)

            if "москва" in address.lower() and "область" not in address.lower():
                log.info("Быстро определен регион: Москва")
                return "region=1"  
                
            if "область" in address.lower():
                log.info("Быстро определен регион: Московская область")
                return "region=4593"  
                
            geocoding_response = self.get_json(CIAN_GEOCODE.format(address))
            
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                if item.get("text", "").startswith("Россия, Моск"):
                    geocoding_result = item
                    break
                    
            if not geocoding_result:
                log.warning(f"Не найдено соответствие для адреса: {address}")
                return "region=4593" if "область" in address.lower() else "region=1"

            lon, lat = geocoding_result.get("coordinates", [0, 0])
                
            if "Московская область" in geocoding_result.get("text", ""):
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "locality"}
                )
                
                try:
                    location_id = for_search_result['details'][1]['id']
                    log.info(f"Определена локация в МО с ID: {location_id}")
                    return f"location[0]={location_id}"
                except (KeyError, IndexError):
                    log.warning(f"Не удалось определить точную локацию в МО: {address}")
                    return "region=4593" 

            else:  
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "district"}
                )
                
                try:
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

                for district_name, district_id in moscow_district_name_to_cian_id.items():
                    if district_name.lower() in address.lower():
                        log.info(f"Найден район в адресе: {district_name}")
                        return f"district[0]={district_id}"

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
                return "region=1"  

        except Exception as e:
            log.exception(f"Ошибка при определении фильтра для адреса {address}: {e}")
            return "region=1" 
    
    def extract_offer_data(self, offer_url, offer_page, lot_uuid, offer_type):
        """Извлекает данные объявления из страницы"""
        try:
            script_match = re.search(r'window\._cianConfig\[\'frontend-offer-card\'\]\.concat\((.*?)\);', offer_page)
            
            if not script_match:
                log.warning(f"Не найден скрипт с данными объявления в {offer_url}")
                return None
                
            json_text = script_match.group(1)
            config = json.loads(json_text)
            
            state_block = next((block for block in config if "key" in block and block["key"] == "defaultState"), None)
            if not state_block:
                log.warning(f"Не найден блок defaultState в {offer_url}")
                return None
                
            state = state_block["value"]
            
            if "offerData" not in state or "offer" not in state["offerData"]:
                log.warning(f"Не найдены данные объявления в {offer_url}")
                return None
                
            offer_data = state["offerData"]["offer"]
            
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
            
            price = 0
            if offer_type.startswith("sale"):
                price = offer_data.get("priceTotalRur", 0)
            else:
                price = offer_data.get("priceTotalPerMonthRur", 0)
                
            if price <= 0:
                log.warning(f"Некорректная цена в {offer_url}")
                return None
                        
            # Улучшенный механизм получения полного адреса
            address = ""
            try:
                # Попытка получить полный адрес из данных объявления
                if offer_data.get("address"):
                    full_address_parts = []
                    
                    # Получаем регион (Москва или другой)
                    geo_data = offer_data.get("geo", {})
                    if "address" in geo_data:
                        address_components = geo_data["address"]
                        # Собираем адрес из компонентов
                        region = next((item.get("title") for item in address_components if item.get("type") == "location"), "")
                        if region:
                            full_address_parts.append(region)
                    
                    # Добавляем район, если есть
                    district = ""
                    for component in offer_data.get("geo", {}).get("districtsInfo", []):
                        if "name" in component:
                            district = component["name"]
                            if district:
                                full_address_parts.append(f"район {district}")
                                break
                    
                    # Добавляем улицу
                    street = offer_data.get("address", {}).get("street", "")
                    if street:
                        full_address_parts.append(street)
                    
                    # Добавляем номер дома
                    house_number = offer_data.get("address", {}).get("house", "")
                    if house_number:
                        full_address_parts.append(f"дом {house_number}")
                    
                    # Собираем полный адрес
                    if full_address_parts:
                        address = ", ".join(full_address_parts)
                
                # Если не удалось получить адрес из данных, пробуем извлечь из DOM
                if not address or len(address) < 10:
                    address_js = """
                    (function() {
                        // Специальная функция для извлечения адреса из DOM
                        function findAddressInText(text) {
                            const patterns = [
                                /г\.\s*Москва[^,]*(,\s*[^,]+){1,5}/i,
                                /Москва[^,]*(,\s*[^,]+){1,5}/i,
                                /г\.\s*Санкт-Петербург[^,]*(,\s*[^,]+){1,5}/i,
                                /Санкт-Петербург[^,]*(,\s*[^,]+){1,5}/i
                            ];
                            
                            for (const pattern of patterns) {
                                const match = text.match(pattern);
                                if (match) return match[0];
                            }
                            return null;
                        }
                        
                        // Ищем адрес по селекторам
                        const selectors = [
                            '[data-name="AddressContainer"]',
                            '[data-name="GeoLabel"]',
                            '.address--GbMIh',
                            '.geo-address--U0Sxb',
                            '.address',
                            '.location-address',
                            '.information__address_text'
                        ];
                        
                        for (const selector of selectors) {
                            const el = document.querySelector(selector);
                            if (el && el.textContent.trim().length > 5)
                                return el.textContent.trim();
                        }
                        
                        // Если не нашли по селекторам, ищем в тексте страницы
                        const allText = document.body.innerText;
                        const addressInText = findAddressInText(allText);
                        if (addressInText) return addressInText;
                        
                        // Ищем метаданные на странице
                        const metaTags = document.querySelectorAll('meta[name="description"], meta[property="og:description"]');
                        for (const tag of metaTags) {
                            const content = tag.getAttribute('content');
                            if (content) {
                                const addressInMeta = findAddressInText(content);
                                if (addressInMeta) return addressInMeta;
                            }
                        }
                        
                        return '';
                    })();
                    """
                    extracted_address = self.driver.execute_script(address_js)
                    if extracted_address and len(extracted_address) > 10:
                        address = extracted_address
                        log.info(f"Извлечен адрес из DOM: {address}")
                    
            except Exception as e:
                log.error(f"Ошибка извлечения адреса: {e}")
            
            # Гарантируем, что адрес будет заполнен
            if not address or len(address) < 10:
                # Используем userInput из geo
                geo_address = offer_data.get('geo', {}).get('userInput', '')
                if geo_address and len(geo_address) > 5:
                    address = f"г. Москва, {geo_address}"
                    log.info(f"Используем адрес из userInput: {address}")
                else:
                    # Берем город из URL и добавляем область видимости
                    city = "Москва"
                    if "spb." in offer_url:
                        city = "Санкт-Петербург"
                    address = f"г. {city}, {geo_address or 'район не определен'}"
                    log.warning(f"Используем запасной адрес: {address}")
            
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
        
        random_params = f"&ad={random.randint(1000, 9999)}&ts={int(time.time())}"
        
        url_templates = [
            (CIAN_SALE_SEARCH, "sale"),
            (CIAN_SALE_SEARCH_LAND, "sale land"),
            (CIAN_RENT_SEARCH, "rent"),
            (CIAN_RENT_SEARCH_LAND, "rent land")
        ]
        
        
        for url_template, offer_type in url_templates:
            log.info(f"Обработка типа объявлений: {offer_type}")
            try:
                search_url = url_template.format(search_filter) + random_params
                log.info(f"Поиск {offer_type}: {search_url}")
                
                self.refresh_main_page()
                
                search_page = self.get_page(search_url)
                if not search_page:
                    log.warning(f"Не удалось получить страницу поиска {offer_type}")
                    continue
                
                save_debug_info(search_url, search_page, self.driver, f"search_{offer_type}")
                
                if "по вашему запросу ничего не найдено" in search_page.lower():
                    log.info(f"Нет результатов для {offer_type}")
                    continue
                
                extracted_offers = self.extract_offers_from_search_page(
                    search_page, search_url, lot_uuid, offer_type
                )
                
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