"""
Полный код cian_minimal.py – оптимизированная версия для стабильной работы
"""

import os
import signal
import time
import logging
import random
import json
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

# Замены для адресов
address_replacements = {
    "р-н": "район",
    "пр-кт": "проспект",
}

# Импортируем модели
from core.models import Lot, Offer

class CianParser:
    def __init__(self):
        # Хранилища для драйвера и главной вкладки
        self.driver = None
        self.first_tab = None
        
        # Инициализируем словарь районов
        self.init_district_mapping()
        
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



    def unformatted_address_to_cian_search_filter(self, address: str) -> str:
        """
        Улучшенный алгоритм преобразования адреса в параметр поискового фильтра ЦИАН.
        
        Теперь учитывает:
        - административные районы города
        - микрорайоны, деревни, посёлки
        - муниципальные округа
        - городские округа
        
        Args:
            address: Текстовый адрес объекта
                
        Returns:
            Строка параметра для поискового URL ЦИАН
        """
        # Проверка кэша
        if not hasattr(self, '_address_filter_cache'):
            self._address_filter_cache = {}
                
        if address in self._address_filter_cache:
            log.info(f"Кэш: Использован сохраненный фильтр для адреса «{address}»")
            return self._address_filter_cache[address]
                
        log.info(f"Определение поискового фильтра для адреса: «{address}»")
        
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

    def extract_offer_data(self, offer_page, offer_url, lot_uuid, offer_type):
        """Извлекает данные о предложении с обработкой ошибок"""
        try:
            offer_soup = BeautifulSoup(offer_page, 'lxml')
            
            # Поиск скрипта с данными объявления
            script_tag = None
            for script in offer_soup.find_all("script"):
                if script.string and "window._cianConfig['frontend-offer-card']" in script.string:
                    script_tag = script
                    break
            
            if not script_tag:
                log.warning(f"Не найден скрипт с данными объявления {offer_url}")
                return None
                
            try:
                # Извлечение JSON данных
                config_json_string = (
                    script_tag.string.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
                )
                config_json = json.loads(config_json_string)
                
                # Поиск блока с данными предложения
                offer_info = None
                for block in config_json:
                    if block.get("key") == "defaultState":
                        offer_info = block.get("value")
                        break
                        
                if not offer_info:
                    log.warning(f"Не найден блок данных объявления {offer_url}")
                    return None
                    
                # Извлечение данных
                area = None
                try:
                    if "land" in offer_info["offerData"]["offer"]:
                        land = offer_info["offerData"]["offer"]["land"]
                        area = float(land["area"])
                        
                        # Преобразуем в квадратные метры в зависимости от единицы измерения
                        if land.get("areaUnitType") == "sotka":
                            area *= 100
                        elif land.get("areaUnitType") == "hectare":
                            area *= 10000
                    else:
                        area = float(offer_info["offerData"]["offer"].get("totalArea", 0))
                except (KeyError, ValueError, TypeError):
                    log.warning(f"Ошибка при извлечении площади для {offer_url}")
                    return None
                    
                # Проверка, что площадь указана
                if not area:
                    log.warning(f"Площадь не указана для {offer_url}")
                    return None
                    
                # Извлекаем адрес и цену
                try:
                    address = offer_info["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                    price = offer_info["offerData"]["offer"].get(
                        "priceTotalRur",
                        offer_info["offerData"]["offer"].get("priceTotalPerMonthRur", 0)
                    )
                    
                    # Создаем объект Offer
                    offer = Offer(
                        id=offer_url.split('/')[-2],
                        lot_uuid=lot_uuid,
                        address=address,
                        area=area,
                        price=price,
                        url=offer_url,
                        type=offer_type
                    )
                    
                    log.info(f"Успешно создано объявление {offer_url}")
                    return offer
                    
                except Exception as e:
                    log.warning(f"Ошибка при создании объекта Offer для {offer_url}: {e}")
                    return None
                    
            except Exception as e:
                log.error(f"Ошибка при обработке JSON: {e}")
                return None
                
        except Exception as e:
            log.error(f"Общая ошибка при обработке объявления {offer_url}: {e}")
            return None


    def parse_nearby_offers(self, search_filter: str, lot_uuid: str) -> Tuple[List[Offer], List[Offer]]:
        """Оптимизированная функция получения объявлений о продаже и аренде"""
        sale_offers = []
        rent_offers = []
        
        # Обрабатываем только важные типы поиска
        search_types = [
            (sale_offers, "sale", CIAN_SALE_SEARCH, "продажа"),
            (rent_offers, "rent", CIAN_RENT_SEARCH, "аренда"),
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
            
            # Ограничиваем количество объявлений для обработки
            """max_offers = 10
            if len(offer_urls) > max_offers:
                log.info(f"Ограничиваем до {max_offers} объявлений")
                offer_urls = offer_urls[:max_offers]"""
            
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
        unique_sale_offers = list({o.id: o for o in sale_offers}.values())
        unique_rent_offers = list({o.id: o for o in rent_offers}.values())
        
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

def fetch_nearby_offers(search_filter: str, lot_uuid: str) -> Tuple[List[Offer], List[Offer]]:
    """Обертка для получения объявлений по фильтру"""
    return get_parser().parse_nearby_offers(search_filter, lot_uuid)

def unformatted_address_to_cian_search_filter(address: str) -> str:
    """Обертка для преобразования адреса в поисковый фильтр"""
    return get_parser().unformatted_address_to_cian_search_filter(address)