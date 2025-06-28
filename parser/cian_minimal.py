"""
Полный код cian_minimal.py – оптимизированная версия для стабильной работы
"""

import os
import signal
import time
import logging
import random
import json
from parser.test_address_cleanup import simplify_address_for_geocoding
from typing import List, Tuple, Dict, Union, Optional, Any
# Добавьте в импорты в начале файла:
import re
from fuzzywuzzy import process, fuzz  # pip install fuzzywuzzy python-Levenshtein
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import orjson
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from fake_useragent import UserAgent
from parser.cian_district import gpt_extract_most_local_cian_part_fixed

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("parser.cian_minimal")

# Константы поиска CIAN
moscow_district_name_to_cian_id: dict[str, int] | None = None


# Базовые URLs и константы
CIAN_MAIN_URL = "https://cian.ru/"
CIAN_DISTRICTS = "https://www.cian.ru/api/geo/get-districts-tree/?locationId=1"
CIAN_GEOCODE = "https://www.cian.ru/api/geo/geocode-cached/?request={}"
CIAN_GEOCODE_FOR_SEARCH = "https://www.cian.ru/api/geo/geocoded-for-search/"
CIAN_SALE_SEARCH = (
    "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2"
    "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3"
    "&office_type[3]=4&office_type[4]=5&office_type[5]=7&office_type[6]=11&{}"
)
CIAN_SALE_SEARCH_LAND = (
    "https://www.cian.ru/cat.php?cats[0]=commercialLandSale&deal_type=sale&engine_version=2&offer_type=offices&{}"
)
CIAN_RENT_SEARCH = (
    "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2"
    "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3"
    "&office_type[3]=4&office_type[4]=5&office_type[5]=7&office_type[6]=11&{}"
)
CIAN_RENT_SEARCH_LAND = (
    "https://www.cian.ru/cat.php?cats[0]=commercialLandRent&deal_type=rent&engine_version=2&offer_type=offices&{}"
)

# Add these imports to the top of cian_minimal.py
from pathlib import Path
from typing import Dict, Optional, Union

# Add these constants after the other CIAN URLs
STREETS_FILE = Path("parser/data/cian_street_ids_simple.json")
STREETS_DETAILED_FILE = Path("parser/data/cian_streets_database.json")

# Global variable to store street mapping
street_name_to_id_mapping: Dict[str, str] = {}

# Замены для адресов
address_replacements = {
    "р-н": "район",
    "пр-кт": "проспект",
}

# Импортируем модели
from core.models import Lot, Offer
# Добавим новую функцию для определения типов недвижимости на ЦИАН в зависимости от категории лота
def get_property_types_for_category(property_category: str) -> tuple[list[str], list[int]]:
    """
    Возвращает список типов коммерческой недвижимости и их кодов для поиска на ЦИАН
    в зависимости от категории лота с торгов.
    
    Args:
        property_category: Категория объекта с торгов
        
    Returns:
        tuple: (список типов для логирования, список кодов для URL запроса)
    """
    # Нормализуем строку для упрощения проверок
    category = property_category.lower().strip() if property_category else ""
    
    # Карта соответствия категорий лотов и типов недвижимости на ЦИАН
    if any(x in category for x in ["нежил", "помещени"]):
        return ["Офис", "Торговая площадь", "Помещение свободного назначения"], [1, 2, 3]
    
    elif any(x in category for x in ["иной объект", "право размещения нто", "нестационар"]):
        return ["Торговая площадь", "Помещение свободного назначения"], [2, 3]
    
    elif any(x in category for x in ["имущественный комплекс", "единый недвижимый комплекс", "сооружени"]):
        return ["Склад", "Производство"], [4, 5]
    
    elif any(x in category for x in ["производственн"]):
        return ["Производство"], [5]
    
    elif any(x in category for x in ["здани", "нежилое здани", "комплекс здани"]):
        return ["Здание"], [6]
    
    elif any(x in category for x in ["земельн", "земли сельхоз", "комплексное развитие территори", "земли населен"]):
        return ["Коммерческая земля"], [7]
    
    elif any(x in category for x in ["торговл", "объект торговли"]):
        return ["Торговая площадь", "Помещение свободного назначения"], [2, 3]
    
    # Дефолтный вариант - все типы коммерческой недвижимости
    return ["Офис", "Торговая площадь", "ПСН", "Склад", "Производство", "Здание", "Земля"], [1, 2, 3, 4, 5, 6, 7]


def load_street_ids() -> Dict[str, str]:
    """Load street ID mapping from file"""
    global street_name_to_id_mapping
    
    if street_name_to_id_mapping:
        # Already loaded
        return street_name_to_id_mapping
    
    try:
        if STREETS_FILE.exists():
            with open(STREETS_FILE, "r", encoding="utf-8") as f:
                street_name_to_id_mapping = json.load(f)
                log.info(f"Loaded {len(street_name_to_id_mapping)} street IDs from {STREETS_FILE}")
        else:
            log.warning(f"Street ID mapping file not found: {STREETS_FILE}")
            street_name_to_id_mapping = {}
    except Exception as e:
        log.error(f"Error loading street IDs: {e}")
        street_name_to_id_mapping = {}
        
    return street_name_to_id_mapping

class CianParser:
    def __init__(self):
        # Хранилища для драйвера и главной вкладки
        self.driver = None
        self.first_tab = None
        
        # Инициализируем словарь районов
        self.init_district_mapping()
        
        # Загружаем кэш улиц
        load_street_ids()
        
        # Создаём кэш для адресных фильтров
        self._address_filter_cache = {}
        
        # Запускаем драйвер
        self.initialize_driver()

    def has_captcha(self, page_source):
        """Улучшенная проверка на капчу с исключением ложных срабатываний"""
        # Более точная проверка на капчу
        captcha_markers = [
            "captcha.yandex.net/image",
            'id="captcha-form"',
            'name="captcha"',
            'class="captcha"',
            "Подтвердите, что вы не робот"
        ]
        
        # Проверяем наличие хотя бы двух маркеров для уверенности
        matches = sum(1 for marker in captcha_markers if marker in page_source)
        
        # Убеждаемся, что это действительно капча, а не просто загрузка страницы
        if matches >= 2:
            log.warning("Обнаружена реальная CAPTCHA!")
            return True
        return False
    
    def init_district_mapping(self):
        """Инициализирует словарь районов Москвы для поиска"""
        global moscow_district_name_to_cian_id
        
        try:
            # Если словарь уже инициализирован, пропускаем
            if moscow_district_name_to_cian_id is not None:
                return
            
            # Создаём временный драйвер если нужно
            temp_driver = None
            if self.driver is None:
                log.info("Создание временного драйвера для получения районов")
                opts = uc.ChromeOptions()
                opts.add_argument(f"--user-agent={UserAgent(browsers=['Chrome']).random}")
                opts.add_argument('--disable-blink-features=AutomationControlled')
                opts.page_load_strategy = 'eager'
                temp_driver = uc.Chrome(options=opts, headless=True)
                temp_driver.set_page_load_timeout(30)
                
            # Используем имеющийся драйвер или временный
            driver = self.driver or temp_driver
            
            # Загружаем данные районов
            try:
                driver.get(CIAN_DISTRICTS)
                time.sleep(2)
                raw = driver.find_element(By.TAG_NAME, 'pre').text
                districts_data = orjson.loads(raw)
                
                # Формируем словарь районов
                moscow_district_name_to_cian_id = {}
                for adm_district in districts_data:
                    if adm_district.get("type") == "Okrug":
                        for district in adm_district.get("childs", []):
                            moscow_district_name_to_cian_id[district["name"]] = district["id"]
                    else:
                        moscow_district_name_to_cian_id[adm_district["name"]] = adm_district["id"]
                
                log.info(f"Успешно загружены данные о {len(moscow_district_name_to_cian_id)} районах Москвы")
            
            except Exception as e:
                log.error(f"Ошибка при загрузке районов: {e}")
                moscow_district_name_to_cian_id = {}
            
            # Закрываем временный драйвер если использовали его
            if temp_driver:
                try:
                    temp_driver.quit()
                except:
                    pass
                    
        except Exception as e:
            log.error(f"Неожиданная ошибка при инициализации районов: {e}")
            moscow_district_name_to_cian_id = {}

    def initialize_driver(self) -> None:
        """Создает новый драйвер Chrome с оптимальными настройками"""
        log.info("Инициализация драйвера Chrome...")
        
        try:
            # Закрываем старый драйвер, если он существует
            if self.driver:
                try:
                    self.driver.quit()
                    if hasattr(self.driver, 'browser_pid'):
                        os.kill(self.driver.browser_pid, signal.SIGKILL)
                        os.kill(self.driver.browser_pid + 1, signal.SIGKILL)
                except:
                    pass
        except Exception:
            pass
        
        # Устанавливаем глобальный таймаут
        os.putenv('GLOBAL_DEFAULT_TIMEOUT', '1200')
        
        # Настраиваем опции Chrome
        opts = uc.ChromeOptions()
        opts.add_argument(f"--user-agent={UserAgent(browsers=['Chrome']).random}")
        opts.add_argument('--disable-blink-features=AutomationControlled')
        opts.page_load_strategy = 'eager'
        
        # Создаём драйвер
        try:
            self.driver = uc.Chrome(options=opts, headless=True)
            self.driver.set_page_load_timeout(300)
            
            # Загружаем главную страницу ЦИАН
            self.driver.get(CIAN_MAIN_URL)
            time.sleep(3)
            
            # Сохраняем первую вкладку
            self.first_tab = self.driver.current_window_handle
            
            # Открываем новую вкладку для работы
            self.driver.switch_to.new_window('tab')
            
            log.info("Драйвер Chrome успешно инициализирован")
        except Exception as e:
            log.error(f"Ошибка при создании драйвера: {e}")
            time.sleep(30)  # Длительная пауза перед повторной попыткой
            self.initialize_driver()  # Рекурсивно пробуем снова
    def find_street_id(self, street_name: str) -> Optional[str]:
        """
        Find street ID from the cached mapping
        
        Args:
            street_name: Street name to look up
            
        Returns:
            Street ID if found, None otherwise
        """
        if not street_name:
            return None
        
        # Load street mapping if not already loaded
        street_mapping = load_street_ids()
        if not street_mapping:
            return None
        
        # Normalize street name for lookup
        street_name_lower = street_name.lower().strip()
        
        # Try direct match first
        for full_name, street_id in street_mapping.items():
            # Extract street name without region
            name_part = full_name.split(" (")[0].lower()
            
            # Check for exact match
            if name_part == street_name_lower:
                log.info(f"Found exact street match: '{street_name}' -> ID: {street_id}")
                return street_id
        
        # Try partial match if exact match failed
        for full_name, street_id in street_mapping.items():
            name_part = full_name.split(" (")[0].lower()
            
            # Check for inclusion
            if street_name_lower in name_part or name_part in street_name_lower:
                log.info(f"Found partial street match: '{street_name}' ~ '{name_part}' -> ID: {street_id}")
                return street_id
                
        # Try fuzzy matching
        try:
            best_match = process.extractOne(
                street_name_lower,
                [name.split(" (")[0].lower() for name in street_mapping.keys()],
                scorer=fuzz.token_sort_ratio,
                score_cutoff=85
            )
            
            if best_match:
                matched_name = best_match[0]
                # Find the original key with this name
                for full_name, street_id in street_mapping.items():
                    if full_name.split(" (")[0].lower() == matched_name:
                        log.info(f"Found fuzzy street match: '{street_name}' ~ '{matched_name}' -> ID: {street_id}")
                        return street_id
        except Exception as e:
            log.warning(f"Error in fuzzy matching for '{street_name}': {e}")
            
        return None
    def refresh_main_page(self):
        """Обновляет главную страницу для сброса состояния"""
        try:
            # Запоминаем текущую вкладку
            current_tab = self.driver.current_window_handle
            
            # Переключаемся на главную вкладку
            self.driver.switch_to.window(self.first_tab)
            
            # Обновляем страницу
            self.driver.refresh()
            time.sleep(3)
            
            # Возвращаемся на исходную вкладку
            self.driver.switch_to.window(current_tab)
            
            log.info("Главная страница успешно обновлена")
        except Exception as e:
            log.warning(f"Ошибка при обновлении главной страницы: {e}")
            try:
                # При проблеме переинициализируем драйвер
                self.initialize_driver()
            except:
                pass

    def refresh_session(self):
        """Полная перезагрузка драйвера для сброса состояния"""
        log.info("Перезагрузка сессии браузера...")
        try:
            self.initialize_driver()
            log.info("Сессия браузера успешно перезагружена")
            return True
        except Exception as e:
            log.error(f"Ошибка при перезагрузке сессии браузера: {e}")
            return False

    def get_page(self, url, retries=0):
        """Загружает страницу с корректной обработкой загрузки"""
        try:
            log.info(f"Загрузка страницы: {url}")
            
            # Устанавливаем большее время ожидания для полной загрузки
            self.driver.set_page_load_timeout(60)
            self.driver.get(url)
            
            # Проверяем статус загрузки
            try:
                # Ждем загрузки DOM
                WebDriverWait(self.driver, 15).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
                
                # Если страница поиска, ждем загрузки карточек объявлений
                if "cat.php" in url:
                    # Ждем появления реальных карточек (не прелоадера)
                    WebDriverWait(self.driver, 20).until(
                        lambda d: len(d.find_elements(By.CSS_SELECTOR, 
                            'div[data-name="OfferPreviewIntersectionObserverComponent"] a')) > 0 or
                        "по вашему запросу ничего не найдено" in d.page_source.lower()
                    )
                    
                    # Скролл для активации ленивой загрузки
                    self.driver.execute_script("window.scrollBy(0, 500)")
                    time.sleep(2)
                
                # Проверяем на наличие CAPTCHA после полной загрузки
                if self.has_captcha(self.driver.page_source):
                    log.warning("Обнаружена капча! Перезагружаем сессию...")
                    self.refresh_session()
                    if retries < 2:
                        time.sleep(5 + random.uniform(0, 3))  # Добавляем случайность
                        return self.get_page(url, retries + 1)
                
                return self.driver.page_source
                
            except TimeoutException:
                log.warning("Таймаут при загрузке страницы, проверяем содержимое...")
                # Проверяем, есть ли хоть что-то полезное на странице
                if len(self.driver.page_source) > 5000:
                    return self.driver.page_source
        
        except Exception as e:
            log.error(f"Ошибка при загрузке страницы {url}: {e}")
            
        # Если дошли до сюда - что-то пошло не так
        if retries < 3:
            log.info(f"Повторная попытка загрузки {url} ({retries+1}/3)")
            self.initialize_driver()
            time.sleep(5)
            return self.get_page(url, retries + 1)
        else:
            log.error(f"Не удалось загрузить страницу {url} после 3 попыток")
            return ""

    def get_json(self, url: str, retries: int = 0) -> dict:
        """Получает и парсит JSON с указанного URL"""
        try:
            self.driver.get(url)
            time.sleep(1)  # Небольшая пауза для загрузки
        except Exception as e:
            log.error(f"Ошибка при загрузке JSON с {url}: {e}")
            if retries < 3:
                self.initialize_driver()
                return self.get_json(url, retries + 1)
            return {}
            
        try:
            # Находим элемент с JSON-данными
            raw = self.driver.find_element(By.TAG_NAME, 'pre').text
            return orjson.loads(raw)
        except Exception as e:
            log.warning(f"Ошибка при парсинге JSON: {e}")
            if retries < 3:
                time.sleep(2)  # Увеличиваем паузу перед повтором
                return self.get_json(url, retries + 1)
            return {}

    def extract_offer_links(self, search_soup):
        """Более надежное извлечение ссылок на объявления"""
        offer_links = []
        
        # Метод 1: Основной селектор
        links = search_soup.find_all("a", attrs={"data-name": "CommercialTitle"})
        if links:
            log.info(f"Найдено {len(links)} ссылок по основному селектору")
            offer_links.extend(links)
        
        # Метод 2: Селектор с id
        if not links:
            links = search_soup.select('#frontend-serp a[href*="/commercial/"]')
            if links:
                log.info(f"Найдено {len(links)} ссылок по селектору с ID")
                offer_links.extend(links)
        
        # Метод 3: По классам ЦИАН
        if not links:
            links = search_soup.select('.c72af68e2a--offerPreview--yhFrR')
            if links:
                log.info(f"Найдено {len(links)} ссылок по классам")
                offer_links.extend(links)
        
        # Метод 4: Самый общий селектор (последний вариант)
        if not links:
            links = search_soup.select('a[href*="/commercial/"][id*="offer"]')
            if links:
                log.info(f"Найдено {len(links)} ссылок по общему селектору")
                offer_links.extend(links)
        
        # Извлекаем URL и удаляем дубликаты
        unique_urls = set()
        result = []
        
        for link in offer_links:
            href = link.get('href')
            if href and "/commercial/" in href and href not in unique_urls:
                if not href.startswith('http'):
                    href = f"https://www.cian.ru{href}"
                unique_urls.add(href)
                result.append(href)
        
        log.info(f"Всего получено {len(result)} уникальных ссылок на объявления")
        return result


    def post_json(self, url: str, body: dict, retries: int = 0) -> dict:
        """Отправляет POST-запрос и получает JSON-ответ"""
        try:
            # Выполняем POST-запрос через JavaScript
            self.driver.execute_script(
                """
                function post(path, params) {
                    const f=document.createElement('form');
                    f.method='post';
                    f.action=path;
                    
                    for(const k in params){
                        const i=document.createElement('input');
                        i.type='hidden';
                        i.name=k;
                        i.value=params[k];
                        f.appendChild(i);
                    }
                    
                    document.body.appendChild(f);
                    f.submit();
                } 
                
                post(arguments[1], arguments[0]);
                """, 
                body, url
            )
            time.sleep(2)  # Пауза для получения ответа
            
        except Exception as e:
            log.error(f"Ошибка при отправке POST-запроса на {url}: {e}")
            if retries < 3:
                self.initialize_driver()
                time.sleep(3)
                return self.post_json(url, body, retries + 1)
            return {}
            
        try:
            # Получаем JSON-ответ
            raw = self.driver.find_element(By.TAG_NAME, 'pre').text
            return orjson.loads(raw)
        except NoSuchElementException:
            if retries < 3:
                time.sleep(2)
                return self.post_json(url, body, retries + 1)
            log.error(f"Не удалось найти элемент с JSON-ответом на {url}")
            return {}
        except Exception as e:
            log.error(f"Ошибка при обработке JSON-ответа: {e}")
            return {}

        """    def unformatted_address_to_cian_search_filter(self, address: str) -> str:
       
        # Проверка кэша
        if address in self._address_filter_cache:
            log.info(f"Использован кэш для адреса: {address}")
            return self._address_filter_cache[address]
            
        log.info(f"Преобразование адреса '{address}' в поисковый фильтр")
        
        # Нормализация адреса
        for old, new in address_replacements.items():
            address = address.replace(old, new)
        
        try:
            # Геокодирование через API ЦИАН
            geocoding_response = self.get_json(CIAN_GEOCODE.format(address))
            
            # Находим первый результат, относящийся к Москве или МО
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                if item.get("text", "").startswith("Россия, Моск"):
                    geocoding_result = item
                    break
            
            # Если не нашли, используем более общий поиск
            if not geocoding_result:
                log.warning(f"Не удалось найти адрес '{address}' через геокодирование")
                result = "region=4593" if "область" in address.lower() else "region=1"
                self._address_filter_cache[address] = result
                return result
            
            lon, lat = geocoding_result["coordinates"]
            
            # Обработка для Московской области
            if "Московская область" in geocoding_result["text"]:
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "locality"}
                )
                
                # Проверяем структуру ответа
                if "details" in for_search_result and len(for_search_result["details"]) > 1:
                    location_id = for_search_result['details'][1]['id']
                    result = f"location[0]={location_id}"
                    log.info(f"Найден населенный пункт в МО, ID: {location_id}")
                    self._address_filter_cache[address] = result
                    return result
                
                # Если не удалось найти конкретный населенный пункт, используем весь регион
                log.warning("Не удалось определить точное местоположение в МО")
                result = "region=4593"
                self._address_filter_cache[address] = result
                return result
                
            # Обработка для Москвы
            else:
                # Пробуем найти район
                for_search_result = self.post_json(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "district"}
                )
                
                # Проверяем, есть ли район в ответе
                try:
                    if "details" in for_search_result and len(for_search_result["details"]) > 2:
                        district_name = (
                            for_search_result["details"][2]["fullName"]
                            .replace("район", "")
                            .replace("р-н", "")
                            .strip()
                        )
                        
                        if district_name in moscow_district_name_to_cian_id:
                            district_id = moscow_district_name_to_cian_id[district_name]
                            result = f"district[0]={district_id}"
                            log.info(f"Найден район '{district_name}', ID: {district_id}")
                            self._address_filter_cache[address] = result
                            return result
                        else:
                            log.warning(f"Район '{district_name}' не найден в справочнике")
                except (KeyError, IndexError) as e:
                    log.warning(f"Ошибка при извлечении района из ответа API: {e}")
                
                # Если район не найден через API, пробуем найти район в адресе напрямую
                try:
                    def key_function(pair):
                        return index if (index := address.lower().find(pair[0].lower())) != -1 else len(address)

                    found_district_name_and_id = min(
                        moscow_district_name_to_cian_id.items(),
                        key=key_function
                    )
                    
                    if key_function(found_district_name_and_id) < len(address):
                        district_name, district_id = found_district_name_and_id
                        result = f"district[0]={district_id}"
                        log.info(f"Найден район в тексте адреса: '{district_name}', ID: {district_id}")
                        self._address_filter_cache[address] = result
                        return result
                except (ValueError, KeyError) as e:
                    log.warning(f"Ошибка при поиске района в тексте адреса: {e}")
                    
                # Если район не найден, пробуем найти улицу
                try:
                    for_search_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "street"}
                    )
                    
                    if "details" in for_search_result and for_search_result["details"]:
                        street_id = for_search_result['details'][-1]['id']
                        street_name = for_search_result['details'][-1].get('fullName', 'не указано')
                        result = f"street[0]={street_id}"
                        log.info(f"Найдена улица '{street_name}', ID: {street_id}")
                        self._address_filter_cache[address] = result
                        return result
                except Exception as e:
                    log.warning(f"Ошибка при поиске улицы: {e}")
                    
                # Если ничего не найдено, используем регион Москва
                log.warning("Не удалось определить точное местоположение, используем общий регион Москва")
                result = "region=1"
                self._address_filter_cache[address] = result
                return result
                
        except Exception as e:
            log.error(f"Общая ошибка при определении местоположения адреса '{address}': {e}")
            # При ошибке используем общий регион в зависимости от наличия слова "область"
            result = "region=4593" if "область" in address.lower() else "region=1"
            self._address_filter_cache[address] = result
            return result"""

    def extract_street_id_from_address(self, address: str) -> Optional[str]:
        """
        Extract street ID from an address using CIAN's geocoding API
        
        Args:
            address: Address text to process
            
        Returns:
            Street ID if found, None otherwise
        """
        try:
            # Step 1: Extract street name from address
            street_name = None
            
            # Try common street patterns
            street_patterns = [
                r'(?:улица|ул\.|ул)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:проспект|пр-т|пр-кт)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:бульвар|б-р)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:шоссе|ш\.)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:переулок|пер\.|пер)\s+([А-Яа-я\-\s]+?)(?:,|\d|$)',
                r'(?:набережная|наб\.|наб)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)',
                r'(?:проезд|пр-д)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)',
                r'(?:площадь|пл\.)\s+([А-Яа-я\-\s]+?)(?:,|\д|$)'
            ]
            
            for pattern in street_patterns:
                match = re.search(pattern, address)
                if match:
                    street_name = match.group(1).strip()
                    break
            
            # If we found a street name directly in the address, try to look it up
            if street_name:
                street_id = self.find_street_id(street_name)
                if street_id:
                    return street_id
            
            # Step 2: If direct lookup failed, use geocoding
            geocoding_response = self.get_json(CIAN_GEOCODE.format(address))
            
            if not geocoding_response or "items" not in geocoding_response:
                log.warning(f"No geocoding results for address: {address}")
                return None
            
            # Find first result for Moscow or Moscow Oblast
            geocoding_result = None
            for item in geocoding_response.get("items", []):
                item_text = item.get("text", "")
                if item_text.startswith("Россия, Моск"):  # Moscow or Moscow Oblast
                    geocoding_result = item
                    break
            
            if not geocoding_result:
                log.warning(f"No Moscow/MO results for address: {address}")
                return None
            
            # Get coordinates
            lon, lat = geocoding_result.get("coordinates", [0, 0])
            
            if lon == 0 or lat == 0:
                log.warning(f"Invalid coordinates for address: {address}")
                return None
            
            # Step 3: Get street ID from coordinates
            api_result = self.post_json(
                CIAN_GEOCODE_FOR_SEARCH,
                {"lat": lat, "lng": lon, "kind": "street"}
            )
            
            if not api_result or "details" not in api_result:
                log.warning(f"No street details for address: {address}")
                return None
            
            # Get details from the result
            details = api_result["details"]
            
            # Find street information (usually the last element)
            for i in range(len(details)-1, -1, -1):
                if "id" in details[i] and "fullName" in details[i]:
                    name_lower = details[i]["fullName"].lower()
                    
                    # Check if this is a street
                    street_markers = ["улица", "переулок", "проспект", "проезд", 
                                    "шоссе", "бульвар", "площадь", "набережная"]
                    
                    if any(marker in name_lower for marker in street_markers):
                        street_id = details[i]["id"]
                        street_name = details[i]["fullName"]
                        
                        log.info(f"Found street via geocoding: {street_name} (ID: {street_id})")
                        return street_id
            
            # If no specific street found, try the last element
            if details and "id" in details[-1]:
                street_id = details[-1]["id"]
                street_name = details[-1].get("fullName", "Unknown")
                
                log.info(f"Using last element as street: {street_name} (ID: {street_id})")
                return street_id
                
        except Exception as e:
            log.error(f"Error extracting street ID for address {address}: {e}")
        
        return None

    # И заменяем функцию unformatted_address_to_cian_search_filter на улучшенную версию:


    def unformatted_address_to_cian_search_filter(self, address: str) -> str:
        
        # Проверка кэша
        if not hasattr(self, '_address_filter_cache'):
            self._address_filter_cache = {}
                
        if address in self._address_filter_cache:
            log.info(f"Кэш: Использован сохраненный фильтр для адреса «{address}»")
            return self._address_filter_cache[address]
                
        log.info(f"Определение поискового фильтра для адреса: «{address}»")
        
        # 1. Первый этап: Попытаемся найти id улицы напрямую по нашему кэшу улиц
        street_id = self.extract_street_id_from_address(address)
        if street_id:
            result = f"street[0]={street_id}"
            log.info(f"Найден ID улицы в кэше: {street_id} для адреса {address}")
            self._address_filter_cache[address] = result
            return result
        
        # 2. Если не нашли улицу в кэше, используем обычный алгоритм геокодирования
        
        # Расширенные замены для нормализации адреса
        extended_replacements = {
            "р-н": "район", "пр-кт": "проспект", "пр-т": "проспект",
            "г.о.": "городской округ", "г/о": "городской округ",
            "мкр.": "микрорайон", "мкрн.": "микрорайон", "мкрн": "микрорайон", "мкр": "микрорайон",
            "п.": "посёлок", "пос.": "посёлок", "поселок": "посёлок",
            "д.": "деревня", "дер.": "деревня",
            "с.": "село", "пгт": "посёлок городского типа", "пгт.": "посёлок городского типа",
            "б-р": "бульвар", "бул.": "бульвар", 
            "обл.": "область", "обл": "область",
            "г.": "город", "гор.": "город"
        }
        
        # Нормализация адреса - более тщательная обработка
        normalized_address = address
        
        # Расширенная замена сокращений
        for old, new in extended_replacements.items():
            # Добавляем пробел после замены, если после сокращения не было пробела
            pattern = rf'\b{re.escape(old)}(?!\s)'
            normalized_address = re.sub(pattern, f"{new} ", normalized_address)
            
            # Стандартная замена с пробелами
            normalized_address = re.sub(rf'\b{re.escape(old)}\s', f"{new} ", normalized_address)
        
        # Убираем лишние пробелы
        normalized_address = re.sub(r'\s+', ' ', normalized_address).strip()
        
        log.info(f"Нормализованный адрес: «{normalized_address}»")
        
        # Работа с нижним регистром для анализа
        address_lower = normalized_address.lower()
        
        # Определение региона (Москва или МО)
        is_likely_moscow = any(marker in address_lower for marker in 
                            ["москва", "вао", "юао", "сао", "зао", "цао", "свао", "юзао", "сзао"])
        is_likely_mo = any(marker in address_lower for marker in 
                            ["московская область", "область московская", "мо", "подмоск"])
        
        # Базовый регион по умолчанию
        default_result = "region=1" if is_likely_moscow else "region=4593" if is_likely_mo else None
        
        # Если регион не определен, пробуем по первым словам
        if not default_result:
            first_part = address_lower.split(',')[0].strip()
            if "москва" in first_part:
                default_result = "region=1"
            elif "область" in first_part and "московск" in first_part:
                default_result = "region=4593"
            else:
                default_result = "region=4593"  # По умолчанию считаем, что это МО
        
        # Если регион не определен, пробуем по первым словам
        if not default_result:
            first_part = address_lower.split(',')[0].strip()
            if "москва" in first_part:
                default_result = "region=1"
            elif "область" in first_part and "московск" in first_part:
                default_result = "region=4593"
            else:
                default_result = "region=4593"  # По умолчанию считаем, что это МО
        # Если регион не определен, пробуем по первым словам
        if not default_result:
            first_part = address_lower.split(',')[0].strip()
            if "москва" in first_part:
                default_result = "region=1"
            elif "область" in first_part and "московск" in first_part:
                default_result = "region=4593"
            else:
                default_result = "region=4593"  # По умолчанию считаем, что это МО
        
        # ИЗВЛЕЧЕНИЕ НАСЕЛЕННЫХ ПУНКТОВ
        # Паттерны для выявления населенных пунктов в МО
        settlement_patterns = {
            'village': r'деревня\s+([А-Яа-яЁё\-]+)',            # деревня Пешково
            'settlement': r'посёлок\s+([А-Яа-яЁё\-]+)',         # посёлок Новый
            'micro_district': r'микрорайон\s+([А-Яа-яЁё\-]+)',  # микрорайон Южный
            'city': r'город\s+([А-Яа-яЁё\-]+)',                # город Химки
            'urban_settlement': r'городское поселение\s+([А-Яа-яЁё\-]+)',  # городское поселение Одинцово
            'rural_settlement': r'сельское поселение\s+([А-Яа-яЁё\-]+)',    # сельское поселение Ершовское
            # Короткие обозначения, где тип населенного пункта указан после имени
            'short_village': r'\s+д\.\s*([А-Яа-яЁё\-]+)',      # д. Пешково или д Пешково
            'short_city': r'\s+г\.\s*([А-Яа-яЁё\-]+)',         # г. Химки или г Химки
            'short_settlement': r'\s+п\.\s*([А-Яа-яЁё\-]+)',    # п. Новый или п Новый
        }
        
        # Поиск населенных пунктов в адресе
        extracted_settlements = {}
        
        for settlement_type, pattern in settlement_patterns.items():
            matches = re.findall(pattern, address_lower)
            if matches:
                for match in matches:
                    extracted_settlements[match.strip()] = settlement_type
                    log.info(f"Найден населенный пункт: {match} (тип: {settlement_type})")
        
        # Поиск городских округов МО
        mo_city_districts = re.findall(r'городской округ\s+([А-Яа-яЁё\-]+)', address_lower)
        if mo_city_districts:
            log.info(f"Найден городской округ МО: {mo_city_districts[0]}")
        
        # ГЕОКОДИРОВАНИЕ
        try:
            # Вызов API геокодирования
            geocoding_response = self.get_json(CIAN_GEOCODE.format(normalized_address))
            
            # Сортировка результатов геокодирования
            moscow_items = []
            mo_items = []
            
            for item in geocoding_response.get("items", []):
                item_text = item.get("text", "")
                if "Россия, Москва" in item_text:
                    moscow_items.append(item)
                elif "Россия, Московская область" in item_text:
                    mo_items.append(item)
            
            # Выбираем приоритетный результат геокодирования
            if is_likely_mo and mo_items:
                # Для Московской области - отдаем приоритет результатам, содержащим найденный населенный пункт
                if extracted_settlements:
                    # Проверяем каждый результат на наличие найденного населенного пункта
                    for settlement_name in extracted_settlements:
                        for item in mo_items:
                            if settlement_name in item.get("text", "").lower():
                                geocoding_result = item
                                log.info(f"Приоритет: результат с населенным пунктом '{settlement_name}'")
                                break
                        if 'geocoding_result' in locals():
                            break
                
                # Если не нашли по населенному пункту, берем первый результат по МО
                if 'geocoding_result' not in locals():
                    geocoding_result = mo_items[0]
                    log.info(f"Используем первый результат по МО: {geocoding_result['text']}")
                    
            elif is_likely_moscow and moscow_items:
                geocoding_result = moscow_items[0]
                log.info(f"Приоритетный результат для Москвы: {geocoding_result['text']}")
            elif mo_items:  # Предполагаем, что это МО, если не указано иное
                geocoding_result = mo_items[0]
                log.info(f"По умолчанию используем результат для МО: {geocoding_result['text']}")
            elif moscow_items:
                geocoding_result = moscow_items[0]
                log.info(f"По умолчанию используем результат для Москвы: {geocoding_result['text']}")
            else:
                # Если нет результатов для Москвы/МО, ищем любой подходящий
                for item in geocoding_response.get("items", []):
                    item_text = item.get("text", "")
                    if item_text.startswith("Россия"):
                        geocoding_result = item
                        log.info(f"Используем общий результат: {item_text}")
                        break
                        
            # Если геокодирование не удалось - возвращаем регион по умолчанию
            if 'geocoding_result' not in locals():
                log.warning(f"Не удалось геокодировать адрес: {address}")
                self._address_filter_cache[address] = default_result
                return default_result

            # Получаем координаты для дальнейшего уточнения
            lon, lat = geocoding_result.get("coordinates", [0, 0])
            
            # Проверка валидности координат
            if lon == 0 or lat == 0:
                log.warning(f"Получены нулевые координаты для адреса: {address}")
                self._address_filter_cache[address] = default_result
                return default_result
                
            # ОПРЕДЕЛЕНИЕ ФИЛЬТРА ПО РЕГИОНУ
            
            # Для Московской области
            if "Московская область" in geocoding_result.get("text", ""):
                log.info("Обработка адреса Московской области")
                
                # Стратегия 1: Попытка определить конкретный населенный пункт
                try:
                    # Запрос к API для определения локации для поиска
                    api_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "locality"}
                    )
                    
                    if api_result and "details" in api_result:
                        # Ищем подходящий населенный пункт или город в ответе API
                        location_found = False
                        details = api_result.get("details", [])
                        
                        # Ищем самый конкретный уровень - с конца списка деталей
                        for i in range(len(details)-1, 0, -1):
                            current = details[i]
                            if not current.get("id"):
                                continue
                                
                            # Получаем название и тип локации
                            location_name = current.get("fullName", "").lower()
                            location_id = current.get("id")
                            
                            # Проверяем, совпадает ли локация с ранее найденными населенными пунктами
                            match_found = False
                            for extracted_name in extracted_settlements:
                                # Проверяем вхождение или нечеткое совпадение
                                if extracted_name in location_name or fuzz.ratio(extracted_name, location_name) > 85:
                                    match_found = True
                                    log.info(f"Найдено соответствие: '{extracted_name}' в '{location_name}'")
                                    break
                            
                            # Если нашли соответствие или это явный населенный пункт
                            is_settlement = any(marker in location_name for marker in 
                                            ["город", "деревня", "посёлок", "село", "микрорайон"])
                                        
                            if match_found or is_settlement or i == len(details)-1:  # Последний элемент - самый конкретный
                                log.info(f"Определена локация в МО: {location_name} (ID: {location_id})")
                                result = f"location[0]={location_id}"
                                self._address_filter_cache[address] = result
                                return result
                        
                        # Если дошли сюда - локация не найдена по критериям, берем первый элемент
                        if details and len(details) > 1 and "id" in details[1]:
                            location_id = details[1]["id"]
                            location_name = details[1].get("fullName", "Неизвестно")
                            log.info(f"Используем общую локацию в МО: {location_name} (ID: {location_id})")
                            result = f"location[0]={location_id}"
                            self._address_filter_cache[address] = result
                            return result
                    else:
                        log.warning("API не вернул информацию о локации")
                
                except Exception as e:
                    log.warning(f"Ошибка при определении локации в МО: {e}")
                
                # Если не удалось определить конкретную локацию - используем весь регион МО
                result = "region=4593"
                self._address_filter_cache[address] = result
                return result
            
            # Для Москвы
            else:
                log.info("Обработка адреса Москвы")
                
                # Стратегия 1: Пытаемся определить район Москвы
                try:
                    api_result = self.post_json(
                        CIAN_GEOCODE_FOR_SEARCH,
                        {"lat": lat, "lng": lon, "kind": "district"}
                    )
                    
                    if api_result and "details" in api_result:
                        details = api_result["details"]
                        
                        # Ищем район (обычно в элементе 2)
                        district_found = False
                        
                        # Перебираем с конца для поиска наиболее конкретного уровня
                        for i in range(len(details)-1, 1, -1):
                            if i < len(details) and "fullName" in details[i]:
                                name_lower = details[i]["fullName"].lower()
                                
                                # Проверка на наличие слова "район" или похожих в названии
                                if any(marker in name_lower for marker in ["район", "р-н"]):
                                    # Извлекаем имя района
                                    district_name = (
                                        details[i]["fullName"]
                                        .replace("район", "")
                                        .replace("р-н", "")
                                        .strip()
                                    )
                                    
                                    # Поиск района в справочнике
                                    district_id = moscow_district_name_to_cian_id.get(district_name)
                                    
                                    # Если не нашли прямое совпадение, используем нечеткий поиск
                                    if not district_id:
                                        match = process.extractOne(
                                            district_name,
                                            moscow_district_name_to_cian_id.keys(),
                                            scorer=fuzz.token_sort_ratio,
                                            score_cutoff=80
                                        )
                                        
                                        if match:
                                            district_name = match[0]
                                            district_id = moscow_district_name_to_cian_id[district_name]
                                    
                                    if district_id:
                                        log.info(f"Определен район Москвы: {district_name} (ID: {district_id})")
                                        result = f"district[0]={district_id}"
                                        district_found = True
                                        self._address_filter_cache[address] = result
                                        return result
                        
                        # Если не нашли район, пробуем улицу
                        if not district_found:
                            try:
                                api_result_street = self.post_json(
                                    CIAN_GEOCODE_FOR_SEARCH,
                                    {"lat": lat, "lng": lon, "kind": "street"}
                                )
                                
                                if api_result_street and "details" in api_result_street and api_result_street["details"]:
                                    details = api_result_street["details"]
                                    
                                    # Ищем улицу с конца списка (наиболее конкретный элемент)
                                    for i in range(len(details)-1, -1, -1):
                                        if "id" in details[i] and "fullName" in details[i]:
                                            name_lower = details[i]["fullName"].lower()
                                            
                                            # Проверяем, что это улица или похожий объект
                                            street_markers = ["улица", "переулок", "проспект", "проезд", 
                                                            "шоссе", "бульвар", "площадь", "набережная"]
                                                            
                                            if any(marker in name_lower for marker in street_markers):
                                                street_id = details[i]["id"]
                                                street_name = details[i]["fullName"]
                                                log.info(f"Определена улица в Москве: {street_name} (ID: {street_id})")
                                                result = f"street[0]={street_id}"
                                                self._address_filter_cache[address] = result
                                                return result
                                    
                                    # Если не нашли явную улицу, берем последний элемент
                                    if len(details) > 0 and "id" in details[-1]:
                                        street_id = details[-1]["id"]
                                        street_name = details[-1].get("fullName", "Неизвестно")
                                        log.info(f"Используем как улицу в Москве: {street_name} (ID: {street_id})")
                                        result = f"street[0]={street_id}"
                                        self._address_filter_cache[address] = result
                                        return result
                            except Exception as e:
                                log.warning(f"Ошибка при определении улицы в Москве: {e}")
                    
                except Exception as e:
                    log.warning(f"Ошибка при определении района в Москве: {e}")
                
                # Если ничего не нашли - используем весь регион Москва
                result = "region=1"
                self._address_filter_cache[address] = result
                return result
                
        except Exception as e:
            log.exception(f"❌ Общая ошибка при определении фильтра для адреса {address}: {e}")
        
        # Если все методы не сработали, используем общий фильтр по региону
        final_region = default_result or "region=1"  # Москва по умолчанию
        log.warning(f"⚠️ Не удалось определить местоположение для: {address}, используем {final_region}")
        
        # Сохраняем в кэш
        self._address_filter_cache[address] = final_region
        return final_region
    

    def extract_area_smart(self, soup, offer_info=None):
        """
        Умное извлечение площади с приоритетом заголовков и валидацией
        """
        log.debug("=== SMART AREA EXTRACTION ===")
        
        # 1. ПРИОРИТЕТ: Площадь из заголовков (title, og:title)
        title_area = self._extract_area_from_titles(soup)
        if title_area:
            log.info(f"✅ Площадь из заголовков: {title_area} м²")
            return title_area
        
        # 2. Площадь из JSON структурированных данных (с валидацией)
        json_area = self._extract_area_from_json(soup, offer_info)
        if json_area and self._validate_area(json_area, title_area):
            log.info(f"✅ Площадь из JSON (валидна): {json_area} м²")
            return json_area
        
        # 3. Площадь из специфических полей (только основные)
        field_area = self._extract_area_from_specific_fields(soup)
        if field_area and self._validate_area(field_area, title_area):
            log.info(f"✅ Площадь из полей (валидна): {field_area} м²")
            return field_area
        
        log.warning("❌ Площадь не найдена или не валидна")
        return None

    def _extract_area_from_titles(self, soup):
        """Извлекает площадь только из заголовков страницы"""
        log.debug("--- Поиск площади в заголовках ---")
        
        title_sources = [
            ("title", soup.find('title')),
            ("og:title", soup.find('meta', {'property': 'og:title'})),
            ("description", soup.find('meta', {'name': 'description'})),
        ]
        
        for source_name, element in title_sources:
            if not element:
                continue
                
            text = element.get('content') if source_name != 'title' else element.get_text()
            if not text:
                continue
                
            log.debug(f"Анализ {source_name}: {text[:100]}...")
            
            # Паттерны для поиска площади в заголовках
            patterns = [
                r'от\s*(\d+(?:[,\.]\d+)?)\s*до\s*(\d+(?:[,\.]\d+)?)\s*м²',  # от X до Y м²
                r'от\s*(\d+(?:[,\.]\d+)?)\s*до\s*(\d+(?:[,\.]\d+)?)м²',     # от X до Yм²
                r'площадью\s*от\s*(\d+(?:[,\.]\d+)?)\s*до\s*(\d+(?:[,\.]\d+)?)\s*м²',
                r'(\d+(?:[,\.]\d+)?)\s*м²',  # просто X м²
                r'(\d+(?:[,\.]\d+)?)м²',     # просто Xм²
                r'(\d+(?:[,\.]\d+)?)\s*кв\.?\s*м',  # X кв.м
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple) and len(match) == 2:
                        # Диапазон площадей - берем максимальную
                        try:
                            area1 = float(match[0].replace(',', '.'))
                            area2 = float(match[1].replace(',', '.'))
                            area = max(area1, area2)
                            log.info(f"Найден диапазон в {source_name}: {area1}-{area2}, выбрана {area}")
                            return area
                        except ValueError:
                            continue
                    else:
                        # Одиночное значение
                        try:
                            area = float(match.replace(',', '.') if isinstance(match, str) else match)
                            if 50 <= area <= 5000:  # Разумные пределы
                                log.info(f"Найдена площадь в {source_name}: {area}")
                                return area
                        except (ValueError, AttributeError):
                            continue
        
        log.debug("Площадь в заголовках не найдена")
        return None

    def _extract_area_from_json(self, soup, offer_info):
        """Извлекает площадь из JSON данных с осторожностью"""
        log.debug("--- Поиск площади в JSON ---")
        
        # 1. Из переданного offer_info
        if offer_info:
            try:
                json_area = offer_info.get("offerData", {}).get("offer", {}).get("totalArea")
                if json_area:
                    area = float(json_area)
                    log.info(f"Площадь из offer_info: {area}")
                    return area
            except (ValueError, TypeError):
                pass
        
        # 2. Из JSON-LD (очень осторожно)
        json_scripts = soup.find_all('script', type='application/ld+json')
        for i, script in enumerate(json_scripts):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    # Ищем только в основных полях
                    for field in ['floorSize', 'area']:
                        if field in data:
                            area = float(data[field])
                            if 50 <= area <= 5000:  # Валидный диапазон
                                log.info(f"Площадь из JSON-LD[{i}].{field}: {area}")
                                return area
            except (json.JSONDecodeError, ValueError, TypeError):
                continue
        
        log.debug("Площадь в JSON не найдена")
        return None

    def _extract_area_from_specific_fields(self, soup):
        """Извлекает площадь только из специфических полей площади"""
        log.debug("--- Поиск площади в специфических полях ---")
        
        # Ищем только в элементах, которые точно относятся к площади
        area_selectors = [
            '[data-testid="areas-table"] .area',  # Таблица площадей
            '.area-value',                        # Значение площади
            '[data-name="AreaValue"]',           # Поле площади
            '.object-area',                      # Площадь объекта
        ]
        
        for selector in area_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text().strip()
                log.debug(f"Проверка {selector}: {text}")
                
                # Ищем числовое значение площади
                area_match = re.search(r'(\d+(?:[,\.]\d+)?)', text)
                if area_match:
                    try:
                        area = float(area_match.group(1).replace(',', '.'))
                        if 50 <= area <= 5000:  # Разумные пределы
                            log.info(f"Площадь из поля {selector}: {area}")
                            return area
                    except ValueError:
                        continue
        
        log.debug("Площадь в специфических полях не найдена")
        return None

    def _validate_area(self, candidate_area, reference_area=None):
        """Валидирует найденную площадь"""
        if not candidate_area:
            return False
        
        # Базовая валидация - разумные пределы
        if not (50 <= candidate_area <= 5000):
            log.warning(f"Площадь {candidate_area} вне разумных пределов")
            return False
        
        # Если есть эталонная площадь из заголовка, проверяем совместимость
        if reference_area:
            # Разрешаем отклонение до 20%
            diff_percent = abs(candidate_area - reference_area) / reference_area * 100
            if diff_percent > 20:
                log.warning(f"Площадь {candidate_area} слишком отличается от заголовка {reference_area} ({diff_percent:.1f}%)")
                return False
        
        return True

    def extract_offer_data(self, offer_page, offer_url, lot_uuid, offer_type):
        """Извлекает данные о предложении с улучшенным извлечением площади"""
        try:
            offer_soup = BeautifulSoup(offer_page, 'lxml')
            
            # Поиск скрипта с данными объявления
            script_tag = None
            offer_info = None
            
            for script in offer_soup.find_all("script"):
                if script.string and "window._cianConfig['frontend-offer-card']" in script.string:
                    script_tag = script
                    break
            
            if script_tag:
                try:
                    # Извлечение JSON данных
                    config_json_string = (
                        script_tag.string.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
                    )
                    config_json = json.loads(config_json_string)
                    
                    # Поиск блока с данными предложения
                    for block in config_json:
                        if block.get("key") == "defaultState":
                            offer_info = block.get("value")
                            break
                except Exception as e:
                    log.warning(f"Ошибка при извлечении JSON из скрипта: {e}")
            
            # ИСПОЛЬЗУЕМ УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ПЛОЩАДИ
            area = self.extract_area_smart(offer_soup, offer_info)
            
            if not area:
                log.warning(f"Площадь не найдена для {offer_url}")
                return None
                
            # Проверка минимальной площади
            if area < 60:
                log.warning(f"Площадь {area} м² меньше минимальной (60 м²) для {offer_url}")
                return None
            
            # Извлекаем адрес и цену
            address = ""
            price = 0
            
            if offer_info:
                try:
                    address = offer_info["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                    price = offer_info["offerData"]["offer"].get(
                        "priceTotalRur",
                        offer_info["offerData"]["offer"].get("priceTotalPerMonthRur", 0)
                    )
                except Exception as e:
                    log.warning(f"Ошибка при извлечении адреса/цены из JSON: {e}")
            
            # Если адрес не найден в JSON, пытаемся найти в HTML
            if not address:
                # Пытаемся найти адрес в breadcrumbs или заголовке
                breadcrumbs = offer_soup.find_all('span', class_='breadcrumbs')
                if breadcrumbs:
                    address = breadcrumbs[-1].get_text().strip()
                else:
                    # Последний шанс - из заголовка
                    title = offer_soup.find('title')
                    if title:
                        title_text = title.get_text()
                        # Простое извлечение адреса из заголовка (после площади)
                        addr_match = re.search(r'м²\s+(.+?),\s+Москва', title_text)
                        if addr_match:
                            address = f"Москва, {addr_match.group(1).strip()}"
            
            # Создаем объект Offer
            offer = Offer(
                id=offer_url.split('/')[-2] if offer_url.endswith('/') else offer_url.split('/')[-1],
                lot_uuid=lot_uuid,
                address=address or "Адрес не указан",
                area=area,
                price=price,
                url=offer_url,
                type=offer_type
            )
            
            log.info(f"✅ Успешно создано объявление {offer_url} (площадь: {area} м²)")
            return offer
            
        except Exception as e:
            log.error(f"Общая ошибка при обработке объявления {offer_url}: {e}")
            return None


    def parse_nearby_offers(self, search_filter: str, lot_uuid: str, property_category: str = "") -> Tuple[List[Offer], List[Offer]]:
        """Оптимизированная функция получения объявлений о продаже и аренде с учетом категории лота"""
        sale_offers = []
        rent_offers = []
        unique_sales = {}
        unique_rents = {}
        
        # Получаем типы недвижимости для поиска на основе категории лота
        property_types, property_codes = get_property_types_for_category(property_category)
        log.info(f"Выбраны типы недвижимости для поиска: {', '.join(property_types)}")
        
        # Формируем URL параметры для типов недвижимости
        types_url_param = "&".join([f"office_type[{i}]={code}" for i, code in enumerate(property_codes)])
        
        # Базовые URL для поиска с учетом выбранных типов
        sale_url_template = (
            "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2"
            f"&offer_type=offices&{types_url_param}&{{}}"
        )
        rent_url_template = (
            "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2"
            f"&offer_type=offices&{types_url_param}&{{}}"
        )

        # Обрабатываем типы поиска
        search_types = [
            (sale_offers, "sale", sale_url_template, "продажа"),
            (rent_offers, "rent", rent_url_template, "аренда"),
        ]
        
        for offers_list, offer_type, url_template, desc in search_types:
            # Добавляем случайность и метку времени для обхода кэширования
            search_url = f"{url_template.format(search_filter)}&_={int(time.time())}&r={random.randint(1000, 9999)}"
            log.info(f"Поиск {desc}: {search_url}")
            
            # Делаем паузу между запросами
            time.sleep(random.uniform(2, 4))
            
            # Получаем страницу поиска
            search_page = self.get_page(search_url)
            if not search_page:
                log.warning(f"Не удалось загрузить страницу поиска для {desc}")
                continue
                
            # Проверка на отсутствие результатов
            if "по вашему запросу ничего не найдено" in search_page.lower():
                log.info(f"Нет результатов для {desc}")
                continue
                
            # Парсим страницу
            search_soup = BeautifulSoup(search_page, 'lxml')
            
            # Получаем ссылки на объявления
            offer_urls = self.extract_offer_links(search_soup)
            
            # Обработка объявлений
            for i, offer_url in enumerate(offer_urls):
                try:
                    log.info(f"Обработка объявления {i+1}/{len(offer_urls)}: {offer_url}")
                    
                    # Добавляем случайные паузы между запросами
                    time.sleep(random.uniform(1.5, 3))
                    
                    # Получаем страницу объявления
                    offer_page = self.get_page(offer_url)
                    if not offer_page:
                        continue
                    
                    # Стандартная обработка страницы объявления
                    offer = self.extract_offer_data(offer_page, offer_url, lot_uuid, offer_type)
                    if offer:
                        offers_list.append(offer)
                        
                except Exception as e:
                    log.error(f"Ошибка при обработке объявления {offer_url}: {e}")
                    continue
        
        # Дедупликация и завершение
        for offer in sale_offers:
            signature = f"{offer.address}|{offer.area}|{offer.price}"
            if offer.id not in unique_sales and signature not in unique_sales.values():
                unique_sales[offer.id] = signature

        for offer in rent_offers:
            signature = f"{offer.address}|{offer.area}|{offer.price}"
            if offer.id not in unique_rents and signature not in unique_rents.values():
                unique_rents[offer.id] = signature
        
        unique_sale_offers = [o for o in sale_offers if o.id in unique_sales]
        unique_rent_offers = [o for o in rent_offers if o.id in unique_rents]
        
        log.info(f"Всего найдено: {len(unique_sale_offers)} объявлений о продаже, {len(unique_rent_offers)} объявлений об аренде")
        return unique_sale_offers, unique_rent_offers

# Глобальная переменная для хранения единственного экземпляра парсера
_parser_instance = None

def get_parser() -> CianParser:
    """Возвращает экземпляр парсера (создает при необходимости)"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = CianParser()
    return _parser_instance

def fetch_nearby_offers(search_filter: str, lot_uuid: str, property_category: str = "") -> Tuple[List[Offer], List[Offer]]:
    """Обертка для получения объявлений по фильтру"""
    return get_parser().parse_nearby_offers(search_filter, lot_uuid, property_category)

def unformatted_address_to_cian_search_filter(address: str) -> str:
    """
    Улучшенная функция преобразования адреса в поисковый фильтр с очисткой дубликатов.
    """
    try:
        # НОВОЕ: Сначала очищаем адрес от дубликатов
        cleaned_address = simplify_address_for_geocoding(address)
        logging.info(f"🧹 Очищенный адрес: '{cleaned_address}' (было: '{address}')")
        
        # Получаем самый локальный элемент адреса
        local_element = gpt_extract_most_local_cian_part_fixed(cleaned_address)
        
        logging.info(f"🎯 Локальный элемент для ЦИАН поиска: '{local_element}' (адрес: '{cleaned_address[:50]}...')")
        
        # Если получили конкретное название улицы или района, используем его для уточненного поиска
        if local_element and local_element not in ["Неизвестно", "Москва", "Московская область"]:
            
            # Если это улица - пробуем найти её ID в кэше
            if any(keyword in cleaned_address.lower() for keyword in ["ул.", "улица", "пр-т", "проспект", "б-р", "бульвар", "наб", "набережная"]):
                street_id = get_parser().find_street_id(local_element)
                if street_id:
                    logging.info(f"✅ Найден ID улицы '{local_element}': {street_id}")
                    return f"street[0]={street_id}"
            
            # Если это район - пробуем найти его ID
            if moscow_district_name_to_cian_id and local_element in moscow_district_name_to_cian_id:
                district_id = moscow_district_name_to_cian_id[local_element]
                logging.info(f"✅ Найден ID района '{local_element}': {district_id}")
                return f"district[0]={district_id}"
        
        # Fallback к старому методу если GPT не дал результата
        logging.info("🔄 Fallback к стандартному методу определения фильтра")
        return get_parser().unformatted_address_to_cian_search_filter(cleaned_address)
        
    except Exception as e:
        logging.error(f"❌ Ошибка при GPT-определении ЦИАН фильтра: {e}")
        # Полный fallback к старому методу
        return get_parser().unformatted_address_to_cian_search_filter(address)