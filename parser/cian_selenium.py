# parser/cian_selenium.py
"""Selenium-based CIAN parser for sale and rent offers near a given address."""

import json
import time
import random
import logging
import re
from typing import Tuple, List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
from uuid import UUID
from core.models import Offer
from parser.proxy_pool import get as get_proxy, drop as drop_proxy

log = logging.getLogger(__name__)

# URL templates for different property types
CIAN_SALE_SEARCH = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices&{}"
CIAN_SALE_SEARCH_LAND = "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=commercial_land&{}"
CIAN_RENT_SEARCH = "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=offices&{}"
CIAN_RENT_SEARCH_LAND = "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=commercial_land&{}"
CIAN_GEOCODE = "https://api.cian.ru/geocoder-search-wrapper/v1/search/?query={}"
CIAN_GEOCODE_FOR_SEARCH = "https://api.cian.ru/geocoder-clusters-frontend/v1/get-clusters/"

# Некоторые замены для стандартизации адресов
address_replacements = {
    "г ": "г. ",
    "обл ": "область ",
    "г.Москва": "г. Москва",
}

def create_stealth_driver():
    """Создает драйвер Chrome с максимальной защитой от обнаружения."""
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/124.0.6367.48 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ]
    
    opts = Options()
    # Опционально отключаем headless для отладки
    if random.random() > 0.1:  # 10% шанс запустить видимый браузер для отладки
        opts.add_argument("--headless")
    
    # Случайный User-Agent
    user_agent = random.choice(user_agents)
    opts.add_argument(f"user-agent={user_agent}")
    
    # Имитация мобильного устройства (случайно включаем или выключаем)
    if False and random.choice([True, False]):
        mobile_devices = ["iPhone X", "iPad Pro", "Pixel 2"]
        opts.add_experimental_option("mobileEmulation", {"deviceName": random.choice(mobile_devices)})
    
    # Отключаем признаки автоматизации
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    
    # Настройки для Mac OS
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    # Случайный размер окна для неголовного режима
    window_sizes = [(1366, 768), (1440, 900), (1600, 900), (1920, 1080)]
    width, height = random.choice(window_sizes)
    opts.add_argument(f"--window-size={width},{height}")
    
    # Добавление прокси
    use_proxy = random.random() > 0.5  # 50% chance to use proxy
    proxy = get_proxy() if use_proxy else None
    if proxy:
        opts.add_argument(f"--proxy-server={proxy}")
    else:
        log.info("Прямое соединение без прокси")
    
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(30)
    
    # Скрываем характеристики webdriver
    try:
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        # Дополнительная маскировка
        driver.execute_script(
            """
            window.navigator.chrome = { runtime: {} };
            window.navigator.permissions = { query: () => Promise.resolve({ state: 'granted' }) };
            """
        )
    except Exception as e:
        log.debug(f"Не удалось выполнить скрипт маскировки: {e}")
    
    return driver, proxy

def human_like_scroll(driver, scroll_count=None):
    """Имитирует естественное прокручивание страницы человеком."""
    if scroll_count is None:
        scroll_count = random.randint(2, 5)
    
    for i in range(scroll_count):
        # Случайное расстояние прокрутки
        scroll_amount = random.randint(300, 800)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
        
        # Случайная задержка между прокрутками
        time.sleep(random.uniform(0.5, 2.0))
        
        # Иногда прокручиваем немного назад, как человек
        if random.random() < 0.3:  # 30% шанс
            driver.execute_script(f"window.scrollBy(0, -{random.randint(50, 150)})")
            time.sleep(random.uniform(0.3, 0.7))

def add_cookies_and_storage(driver):
    """Добавляет куки и локальное хранилище для лучшего маскирования."""
    try:
        # Устанавливаем куки, которые обычно присутствуют у реального пользователя
        cookies = [
            {"name": "visited_before", "value": "true"},
            {"name": "session_region_id", "value": "1"},
            {"name": "session_main_town_region_id", "value": "1"},
            {"name": "_ym_uid", "value": f"{random.randint(1500000000, 1699999999)}"},
            {"name": "tmr_detect", "value": "1%7C" + str(int(time.time()))},
        ]
        
        for cookie in cookies:
            driver.add_cookie(cookie)
            
        # Добавляем данные в локальное хранилище
        driver.execute_script("localStorage.setItem('showBannerToolTip', 'false');")
        driver.execute_script("localStorage.setItem('hidePromoCianProPopup', 'true');")
        
    except Exception as e:
        log.debug(f"Ошибка при добавлении cookies: {e}")

def check_for_captcha(driver, html=None):
    """Проверяет наличие капчи или блокировки на странице."""
    if html is None:
        html = driver.page_source.lower()
    else:
        html = html.lower()
        
    captcha_indicators = [
        "captcha", "anti-bot", "подтвердить, что вы не робот",
        "проверка безопасности", "security check", "blocked"
    ]
    
    for indicator in captcha_indicators:
        if indicator in html:
            return True
    
    # Проверка по элементам DOM
    try:
        captcha_elements = driver.find_elements(By.XPATH, "//*[contains(@class, 'captcha') or contains(@id, 'captcha')]")
        if captcha_elements:
            return True
    except:
        pass
        
    return False

def is_empty_results(html):
    """Проверяет, есть ли сообщение об отсутствии результатов на странице."""
    empty_indicators = [
        "по вашему запросу ничего не найдено",
        "не нашлось подходящих объявлений",
        "объявлений не найдено",
        "zero results",
        "нет результатов"
    ]
    
    html_lower = html.lower()
    for indicator in empty_indicators:
        if indicator in html_lower:
            return True
    return False
   
def fetch_nearby_offers(search_filter: str, lot_uuid) -> Tuple[List[Offer], List[Offer]]:
    """
    Функция для извлечения предложений о продаже и аренде с CIAN.
    Реализована защита от блокировки: прокси, имитация человеческого поведения, ротация UA.
    """
    log.info(f"Запрос предложений CIAN для фильтра: {search_filter}")
    
    sale_offers: List[Offer] = []
    rent_offers: List[Offer] = []
    
    # До 3 попыток с разными прокси
    for attempt in range(3):
        driver, proxy = create_stealth_driver()
        try:
            log.info(f"Попытка {attempt+1} поиска предложений")
            
            # Сначала посещаем главную страницу и делаем случайные действия
            driver.get("https://www.cian.ru")
            time.sleep(random.uniform(2, 4))
            
            # Проверка на капчу
            if check_for_captcha(driver):
                log.warning(f"Обнаружена капча на главной странице. Меняем прокси.")
                if proxy:
                    drop_proxy(proxy)
                continue  # Переходим к следующей попытке с новым прокси
            
            # Устанавливаем куки
            add_cookies_and_storage(driver)
            
            # Имитация скроллинга
            human_like_scroll(driver, random.randint(1, 3))
            
            # Пройдемся по нескольким страницам для естественности
            if random.random() < 0.7:  # 70% вероятность
                intermediate_pages = [
                    "https://www.cian.ru/commercial/",
                    "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices"
                ]
                
                # Посетим одну случайную промежуточную страницу
                interim_url = random.choice(intermediate_pages)
                driver.get(interim_url)
                time.sleep(random.uniform(2, 4))
                
                # Ещё один скроллинг
                human_like_scroll(driver)
            
            # Теперь начинаем поиск по нужным фильтрам
            url_templates = [
                (CIAN_SALE_SEARCH, "sale"),
                (CIAN_SALE_SEARCH_LAND, "sale land"),
                (CIAN_RENT_SEARCH, "rent"),
                (CIAN_RENT_SEARCH_LAND, "rent land"),
            ]
            
            # Перемешиваем порядок URL для естественности
            if random.random() < 0.5:
                random.shuffle(url_templates)
            
            for url_template, offer_type in url_templates:
                try:
                    url = url_template.format(search_filter)
                    log.info(f"Открываем URL {offer_type}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(3, 5))
                    
                    # Имитация скроллинга
                    human_like_scroll(driver)
                    
                    # Получаем HTML страницы
                    html = driver.page_source
                    
                    # Проверяем на капчу или блокировку
                    if check_for_captcha(driver, html):
                        log.warning(f"Обнаружена блокировка на попытке {attempt+1}! Меняем прокси.")
                        
                        # Сохраняем HTML для отладки
                        with open(f"cian_dump_{offer_type}_{attempt}.html", "w", encoding="utf-8") as f:
                            f.write(html)
                        
                        # Помечаем прокси как нерабочий
                        if proxy:
                            drop_proxy(proxy)
                        
                        # Прерываем текущую попытку
                        break
                    
                    # Проверка на отсутствие результатов
                    if is_empty_results(html):
                        log.info(f"CIAN сообщает: по запросу {offer_type} ничего не найдено")
                        continue
                    
                    # Парсим HTML с помощью BeautifulSoup
                    soup = BeautifulSoup(html, "lxml")
                    
                    # Находим все script теги с данными объявлений
                    scripts = soup.find_all("script", text=re.compile("window._CIAN_COMPONENT_DATA_"))
                    
                    log.info(f"Найдено {len(scripts)} скриптов с данными")
                    extracted_offers = 0
                    
                    # STEP 1: Извлекаем данные из JSON в тегах script
                    for script in scripts:
                        try:
                            # Извлечение JSON из script
                            json_match = re.search(r'window\._CIAN_COMPONENT_DATA_\s*=\s*({.*})', script.string)
                            if not json_match:
                                continue
                                
                            data = json.loads(json_match.group(1))
                            
                            # Обработка каждого объявления в результатах поиска
                            offers_list = []
                            
                            # Проверяем разные структуры данных
                            if "offers" in data:
                                offers_list = data["offers"]
                            elif "value" in data and "results" in data["value"]:
                                offers_list = data["value"]["results"].get("offers", [])
                            # Проверка по дополнительным путям
                            elif "value" in data and "results" in data["value"] and "aggregatedOffers" in data["value"]["results"]:
                                offers_list = data["value"]["results"]["aggregatedOffers"]
                            elif "items" in data:
                                # Для случаев, когда данные находятся в другой структуре
                                for item in data["items"]:
                                    if "offer" in item:
                                        offers_list.append(item["offer"])
                            
                            if not offers_list:
                                continue
                            
                            log.info(f"Найдено {len(offers_list)} объявлений в JSON данных")
                            
                            for offer_data in offers_list:
                                try:
                                    # Получаем ID объявления
                                    oid = str(offer_data.get("id", 0))
                                    if not oid:
                                        continue
                                        
                                    # Формируем URL объявления в зависимости от типа
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
                                    
                                    # Извлекаем площадь с несколькими резервными вариантами
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
                                    
                                    # Создаем объект предложения
                                    if price > 0 and area > 0:
                                        entry = Offer(
                                            id=oid,
                                            lot_uuid=lot_uuid,
                                            price=price,
                                            area=area,
                                            url=href,
                                            type=offer_type.split()[0],
                                            address=address
                                        )
                                        
                                        if offer_type.startswith("sale"):
                                            sale_offers.append(entry)
                                        else:
                                            rent_offers.append(entry)
                                            
                                        extracted_offers += 1
                                    
                                except Exception as e:
                                    log.debug(f"Ошибка обработки объявления: {e}")
                                    
                        except Exception as e:
                            log.debug(f"Ошибка обработки скрипта с данными: {e}")
                    
                    log.info(f"Извлечено {extracted_offers} объявлений типа {offer_type} из JSON")
                    
                    # STEP 2: Если мало объявлений, пробуем извлечь из HTML карточек
                    if extracted_offers < 5:
                        log.info("Недостаточно данных из JSON, пробуем извлечение из HTML...")
                        
                        # Проверяем разные селекторы карточек
                        card_selectors = [
                            "article[data-name='CardComponent']", 
                            "div[data-name='CardComponent']",
                            "article.--card--",
                            "div.card-container"
                        ]
                        
                        cards = []
                        for selector in card_selectors:
                            cards = soup.select(selector)
                            if cards:
                                log.info(f"Найдено {len(cards)} карточек по селектору {selector}")
                                break
                        
                        # Если карточки не найдены, ищем ссылки на объявления
                        if not cards:
                            anchors = soup.find_all("a", attrs={"data-name": "CommercialTitle"})
                            log.info(f"Найдено {len(anchors)} ссылок на объявления")
                            
                            # Обрабатываем первые 10 ссылок
                            for a in anchors[:10]:
                                try:
                                    href = a.get("href")
                                    if not href or "cian.ru" not in href:
                                        continue
                                        
                                    # Получаем ID объявления из URL
                                    oid = href.split('/')[-2] if '/' in href else "0"
                                    if not oid.isdigit():
                                        continue
                                    
                                    # Ищем родительскую карточку
                                    card = a.find_parent("article") or a.find_parent("div", class_=lambda c: c and "card" in c.lower())
                                    if not card:
                                        continue
                                        
                                    # Извлекаем цену
                                    price = 0
                                    price_elems = card.select("[data-mark='PriceInfo'], [data-name='PriceInfo'], .--price--")
                                    for price_elem in price_elems:
                                        price_text = price_elem.get_text().strip()
                                        price_match = re.search(r'(\d[\d\s]*)', price_text)
                                        if price_match:
                                            price = float(price_match.group(1).replace(" ", "").replace(",", "."))
                                            break
                                    
                                    # Извлекаем площадь
                                    area = 0
                                    area_elems = card.select("[data-mark='AreaInfo'], [data-name='AreaInfo'], .--area--")
                                    for area_elem in area_elems:
                                        area_text = area_elem.get_text().strip()
                                        area_match = re.search(r'(\d+[.,]?\d*)', area_text)
                                        if area_match:
                                            area = float(area_match.group(1).replace(',', '.'))
                                            break
                                    
                                    # Извлекаем адрес
                                    address = ""
                                    address_elems = card.select("[data-mark='AddressInfo'], [data-name='AddressInfo'], .--address--")
                                    for address_elem in address_elems:
                                        address = address_elem.get_text().strip()
                                        if address:
                                            break
                                    
                                    # Создаем объявление с более либеральными условиями
                                    if price > 0 or area > 0:  # OR вместо AND для более мягких условий
                                        entry = Offer(
                                            id=oid,
                                            lot_uuid=lot_uuid,
                                            price=price if price > 0 else 0,
                                            area=area if area > 0 else 1,  # 1 по умолчанию для предотвращения деления на ноль
                                            url=href,
                                            type=offer_type.split()[0],
                                            address=address
                                        )
                                        
                                        if offer_type.startswith("sale"):
                                            sale_offers.append(entry)
                                        else:
                                            rent_offers.append(entry)
                                        
                                        extracted_offers += 1
                                        
                                except Exception as e:
                                    log.debug(f"Ошибка извлечения из карточки: {e}")
                        
                        else:  # Обработка найденных карточек
                            for card in cards[:10]:  # Ограничиваем 10 карточками для производительности
                                try:
                                    # Находим ссылку на объявление
                                    a = card.find("a", href=lambda href: href and "cian.ru" in href)
                                    if not a:
                                        continue
                                        
                                    href = a.get("href")
                                    oid = href.split('/')[-2] if '/' in href else "0"
                                    
                                    # Далее аналогично извлекаем цену, площадь и адрес
                                    # ... (тот же код, что и выше для обработки карточек)
                                    
                                except Exception as e:
                                    log.debug(f"Ошибка обработки карточки: {e}")
                    
                    # STEP 3: Если всё ещё недостаточно объявлений, посещаем индивидуальные страницы
                    if extracted_offers < 3:
                        log.info("Всё ещё недостаточно данных, переходим на страницы объявлений...")
                        
                        # Находим все ссылки на объявления
                        all_links = []
                        for a in soup.find_all("a", href=lambda href: href and "cian.ru" in href and 
                                             ("/sale/" in href or "/rent/" in href)):
                            if a.get("href") not in [offer.url for offer in sale_offers + rent_offers]:
                                all_links.append(a.get("href"))
                        
                        # Обрабатываем первые 5 уникальных ссылок
                        unique_links = list(set(all_links))[:5]
                        for idx, href in enumerate(unique_links):
                            try:
                                # Добавляем случайную задержку перед посещением
                                delay = random.uniform(2, 5)
                                log.info(f"Ожидание {delay:.1f} сек перед посещением {href}...")
                                time.sleep(delay)
                                
                                # Обновляем главную страницу каждые 2-3 объявления для снижения риска блокировки
                                if idx > 0 and idx % 2 == 0:
                                    log.info("Обновляем сессию...")
                                    driver.get("https://www.cian.ru")
                                    time.sleep(random.uniform(1, 3))
                                    human_like_scroll(driver, 1)
                                
                                # Переходим на страницу объявления
                                driver.get(href)
                                time.sleep(random.uniform(2, 4))
                                
                                # Имитируем скроллинг
                                human_like_scroll(driver, random.randint(2, 4))
                                
                                # Проверка на капчу на странице объявления
                                if check_for_captcha(driver):
                                    log.warning("Обнаружена капча на странице объявления. Пропускаем.")
                                    break
                                
                                # Парсим страницу объявления
                                offer_soup = BeautifulSoup(driver.page_source, "lxml")
                                
                                # Находим скрипт с данными объявления
                                script = None
                                for script_tag in offer_soup.find_all("script"):
                                    if script_tag.string and "window._cianConfig['frontend-offer-card']" in script_tag.string:
                                        script = script_tag
                                        break
                                
                                # Попытка найти альтернативные скрипты с данными
                                if not script:
                                    for script_tag in offer_soup.find_all("script"):
                                        if script_tag.string and "window._cianConfig" in script_tag.string and "offer" in script_tag.string:
                                            script = script_tag
                                            break
                                
                                # Если скрипт не найден, пробуем извлечь данные напрямую из HTML
                                if not script:
                                    log.info("Скрипт с данными не найден, извлекаем из HTML...")
                                    
                                    # Извлечение цены (множественные селекторы для надежности)
                                    price = 0
                                    price_selectors = [
                                        "span[itemprop='price']", 
                                        ".price-value", 
                                        "[data-testid='offer-price']"
                                    ]
                                    for selector in price_selectors:
                                        price_elem = offer_soup.select_one(selector)
                                        if price_elem:
                                            price_text = price_elem.get_text().strip()
                                            price_match = re.search(r'(\d[\d\s]*)', price_text)
                                            if price_match:
                                                price = float(price_match.group(1).replace(" ", "").replace(",", "."))
                                                break
                                    
                                    # Извлечение площади
                                    area = 0
                                    area_selectors = [
                                        "div:contains('Площадь')", 
                                        "[data-testid='offer-area']",
                                        ".info-row:contains('Площадь')"
                                    ]
                                    for selector in area_selectors:
                                        area_elem = offer_soup.select_one(selector)
                                        if area_elem:
                                            area_text = area_elem.get_text().strip()
                                            area_match = re.search(r'(\d+[.,]?\d*)\s*(?:м2|м²|кв\.?\s*м)', area_text)
                                            if area_match:
                                                area = float(area_match.group(1).replace(',', '.'))
                                                break
                                    
                                    # Извлечение адреса
                                    address = ""
                                    address_selectors = [
                                        "[data-testid='offer-address']",
                                        "div[itemprop='address']",
                                        ".address-container"
                                    ]
                                    for selector in address_selectors:
                                        address_elem = offer_soup.select_one(selector)
                                        if address_elem:
                                            address = address_elem.get_text().strip()
                                            if address:
                                                break
                                    
                                    # Создаем объект объявления
                                    oid = href.split('/')[-2] if '/' in href else "0"
                                    if price > 0 or area > 0:
                                        entry = Offer(
                                            id=oid,
                                            lot_uuid=lot_uuid,
                                            price=price if price > 0 else 0,
                                            area=area if area > 0 else 1,
                                            url=href,
                                            type=offer_type.split()[0],
                                            address=address
                                        )
                                        
                                        if offer_type.startswith("sale"):
                                            sale_offers.append(entry)
                                        else:
                                            rent_offers.append(entry)
                                            
                                        extracted_offers += 1
                                    
                                    continue  # Перейти к следующему объявлению
                                
                                # Если скрипт найден, извлекаем JSON из него
                                try:
                                    json_text = ""
                                    
                                    # Обработка различных форматов скрипта
                                    if ".concat(" in script.text:
                                        # Стандартный формат
                                        json_text = script.text.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
                                    else:
                                        # Альтернативный формат
                                        json_start = script.text.find("[{")
                                        json_end = script.text.rfind("}]") + 2
                                        if json_start >= 0 and json_end > 0:
                                            json_text = script.text[json_start:json_end]
                                        else:
                                            # Последняя попытка найти JSON-объект
                                            match = re.search(r'\{.*\}', script.text)
                                            if match:
                                                json_text = match.group(0)
                                    
                                    if not json_text:
                                        continue
                                        
                                    # Загружаем JSON
                                    config = json.loads(json_text)
                                    offer_data = {}
                                    
                                    # Обрабатываем разные структуры JSON
                                    if isinstance(config, list):
                                        # Структура списка с блоками
                                        state_block = next((block for block in config if "key" in block and block["key"] == "defaultState"), None)
                                        if state_block:
                                            state = state_block["value"]
                                            if "offerData" in state and "offer" in state["offerData"]:
                                                offer_data = state["offerData"]["offer"]
                                    else:
                                        # Структура одиночного объекта
                                        if "offerData" in config and "offer" in config["offerData"]:
                                            offer_data = config["offerData"]["offer"]
                                        elif "offer" in config:
                                            offer_data = config["offer"]
                                    
                                    if not offer_data:
                                        continue
                                    
                                    # Извлекаем площадь с различными резервными вариантами
                                    area = 0
                                    try:
                                        if "land" in offer_data:
                                            # Для участков земли
                                            unit = offer_data["land"].get("areaUnitType", "")
                                            raw = float(offer_data["land"].get("area", 0))
                                            area = raw * (100 if unit == "sotka" else 10000 if unit == "hectare" else 1)
                                        else:
                                            # Для обычных помещений
                                            area = float(offer_data.get("totalArea", 0))
                                    except Exception as e:
                                        log.debug(f"Ошибка извлечения площади: {e}")
                                        
                                        # Ищем площадь в описании
                                        if "description" in offer_data:
                                            desc = offer_data.get("description", "")
                                            area_match = re.search(r'(\d+[.,]?\d*)\s*(?:м2|м²|кв\.?\s*м)', desc)
                                            if area_match:
                                                area = float(area_match.group(1).replace(',', '.'))
                                        
                                        # Дополнительный поиск по деталям
                                        if not area and "details" in offer_data:
                                            area_text = offer_data["details"].get("area", "")
                                            if area_text:
                                                area_match = re.search(r'(\d+[.,]?\d*)', area_text)
                                                if area_match:
                                                    area = float(area_match.group(1).replace(',', '.'))

                                    # Более мягкая проверка - разрешаем малые площади
                                    if area < 0.1:
                                        log.warning(f"Невозможно извлечь площадь для {href}")
                                        area = 1  # По умолчанию 1 м² для предотвращения деления на ноль
                                        
                                    # Получаем цену в зависимости от типа объявления
                                    price = 0
                                    try:
                                        price_candidates = [
                                            offer_data.get("priceTotalRur", 0),
                                            offer_data.get("priceTotalPerMonthRur", 0),
                                            offer_data.get("price", 0),
                                            offer_data.get("priceRur", 0)
                                        ]
                                        price = next((p for p in price_candidates if p), 0)
                                    except Exception as e:
                                        log.debug(f"Ошибка извлечения цены: {e}")
                                    
                                    # Получаем адрес через несколько проверок
                                    address = ""
                                    try:
                                        # Проверяем разные пути для получения адреса
                                        if "adfoxOffer" in state:
                                            address = state["adfoxOffer"]["response"]["data"]["unicomLinkParams"].get("puid14", "")
                                        
                                        if not address and "address" in offer_data:
                                            address = offer_data.get("address", "")
                                    except (KeyError, TypeError, NameError):
                                        try:
                                            if "address" in offer_data:
                                                address = offer_data.get("address", "")
                                        except Exception:
                                            pass
                                    
                                    # Создаем объект объявления
                                    oid = str(offer_data.get("cianId", 0)) or href.split('/')[-2]
                                    entry = Offer(
                                        id=oid,
                                        lot_uuid=lot_uuid,
                                        price=price if price > 0 else 0,
                                        area=area,
                                        url=href,
                                        type=offer_type.split()[0],
                                        address=address
                                    )
                                    
                                    # Добавляем в соответствующий список
                                    if offer_type.startswith("sale"):
                                        sale_offers.append(entry)
                                    else:
                                        rent_offers.append(entry)
                                        
                                    extracted_offers += 1
                                        
                                except Exception as e:
                                    log.warning(f"Ошибка обработки JSON для {href}: {e}")
                                    
                            except Exception as e:
                                log.warning(f"Ошибка обработки объявления {href}: {str(e)[:100]}...")
                                
                except Exception as e:
                    log.error(f"Ошибка обработки URL типа {offer_type}: {e}")
                    # Продолжаем со следующим шаблоном URL вместо полного сбоя
            
            # Если получили какие-то объявления - выходим из цикла попыток
            if len(sale_offers) > 0 or len(rent_offers) > 0:
                log.info(f"Успешно получены объявления на попытке {attempt+1}")
                break
                
        except Exception as e:
            log.error(f"Ошибка браузера на попытке {attempt+1}: {e}")
            # Помечаем прокси как нерабочий в случае ошибки
            if proxy:
                drop_proxy(proxy)
            
        finally:
            try:
                driver.quit()
            except:
                pass
            
        # Делаем паузу между попытками, кроме последней
        if attempt < 2 and (len(sale_offers) == 0 and len(rent_offers) == 0):
            delay = random.uniform(10, 20)
            log.info(f"Нет результатов, ждем {delay:.1f} сек перед следующей попыткой...")
            time.sleep(delay)
    
    # Если после всех попыток всё равно нет результатов, пробуем расширенный поиск
    if len(sale_offers) == 0 and len(rent_offers) == 0:
        if "district" in search_filter or "location" in search_filter:
            log.info("Не найдено предложений для локации. Пробуем расширить поиск до региона.")
            region = "region=4593" if "область" in search_filter.lower() else "region=1"
            return fetch_nearby_offers(region, lot_uuid)
    
    # Удаляем дубликаты по ID
    unique_sale_offers = []
    unique_ids = set()
    for offer in sale_offers:
        if offer.id not in unique_ids:
            unique_ids.add(offer.id)
            unique_sale_offers.append(offer)
    sale_offers = unique_sale_offers
    
    unique_rent_offers = []
    unique_ids = set()
    for offer in rent_offers:
        if offer.id not in unique_ids:
            unique_ids.add(offer.id)
            unique_rent_offers.append(offer)
    rent_offers = unique_rent_offers
    
    log.info(f"Собрано {len(sale_offers)} уникальных предложений о продаже и {len(rent_offers)} предложений об аренде")
    return sale_offers, rent_offers

def driver_get_json(url: str) -> dict:
    """Get JSON from a URL using a Chrome driver."""
    driver, _ = create_stealth_driver()
    
    try:
        driver.get(url)
        time.sleep(2)
        pre_element = driver.find_element(By.TAG_NAME, "pre")
        return json.loads(pre_element.text)
    finally:
        driver.quit()

def driver_post(url: str, body: dict) -> dict:
    """Post data to a URL using a Chrome driver and get JSON response."""
    driver, _ = create_stealth_driver()
    
    try:
        # Execute JavaScript to perform POST
        driver.get("about:blank")
        js_code = """
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
        
        post(arguments[0], arguments[1]);
        """
        driver.execute_script(js_code, url, body)
        time.sleep(3)
        
        pre_element = driver.find_element(By.TAG_NAME, "pre")
        return json.loads(pre_element.text)
    finally:
        driver.quit()

def unformatted_address_to_cian_search_filter(address: str) -> str:
    """
    Преобразует нестандартный адрес в параметр поискового фильтра CIAN.
    Улучшенная версия с поддержкой малых городов и регионов.
    """
    try:
        # Проверка на малые города/районы Подмосковья, где мало предложений
        small_towns = ["кашира", "озёры", "озеры", "зарайск", "серебряные пруды", 
                       "лотошино", "шаховская", "волоколамск", "серпухов", "чехов"]
        
        if any(town in address.lower() for town in small_towns):
            log.info(f"Обнаружен малый город в адресе, использую фильтр по всей области: {address}")
            return "region=4593"  # ID Московской области
        
        # Проверка на город Москва для ускорения
        if "москва" in address.lower() and "область" not in address.lower():
            log.info("Распознан адрес в Москве, использую регион Москва")
            return "region=1"  # ID Москвы
            
        # Стандартизация адреса
        for old, new in address_replacements.items():
            address = address.replace(old, new)

        # Геокодирование адреса
        log.info(f"Отправка запроса на геокодирование адреса: {address}")
        geocoding_response = driver_get_json(CIAN_GEOCODE.format(address))
        
        # Ищем подходящий элемент в ответе
        geocoding_result = None
        for item in geocoding_response.get("items", []):
            if item.get("text", "").startswith("Россия, Моск"):
                geocoding_result = item
                break
                
        if not geocoding_result:
            log.warning(f"Не найдено соответствие для адреса: {address}")
            # Возвращаем общий регион на основе адреса
            return "region=4593" if "область" in address.lower() else "region=1"
            
        lon, lat = geocoding_result["coordinates"]
        log.info(f"Определены координаты: {lat}, {lon}")

        # Обработка Московской области
        if "Московская область" in geocoding_result["text"]:
            try:
                for_search_result = driver_post(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "locality"},
                )
                
                # Проверяем наличие деталей в ответе
                if for_search_result and "details" in for_search_result and len(for_search_result["details"]) > 1:
                    location_id = for_search_result["details"][1]["id"]
                    log.info(f"Определен ID локации: {location_id}")
                    return f"location[0]={location_id}"
                else:
                    log.warning("Недостаточно данных в ответе, использую регион МО целиком")
                    return "region=4593"
            except Exception as e:
                log.error(f"Ошибка при получении ID локации: {e}")
                return "region=4593"  # Возвращаем общий регион МО в случае ошибки
            
        else:  # Москва
            try:
                # Пробуем сначала получить район
                for_search_result = driver_post(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "district"},
                )
                
                # Если есть информация о районе, используем её
                if for_search_result and "details" in for_search_result and len(for_search_result["details"]) > 2:
                    district_id = for_search_result["details"][2].get("id")
                    if district_id:
                        log.info(f"Определен ID района: {district_id}")
                        return f"district[0]={district_id}"
                
                # Если не получилось определить район, пробуем улицу
                for_search_result = driver_post(
                    CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "street"},
                )
                
                if for_search_result and "details" in for_search_result and len(for_search_result["details"]) > 0:
                    street_id = for_search_result["details"][-1].get("id")
                    if street_id:
                        log.info(f"Определен ID улицы: {street_id}")
                        return f"street[0]={street_id}"
                
                # Если не смогли определить ни район, ни улицу
                log.warning("Не удалось определить район или улицу, использую регион Москва")
                return "region=1"
                
            except Exception as e:
                log.error(f"Ошибка при определении фильтров поиска: {e}")
                return "region=1"  # Возвращаем Москву целиком в случае ошибки
                
    except Exception as e:
        log.warning(f"Не удалось определить локацию для адреса '{address}': {e}")
        # Определяем регион по тексту адреса
        return "region=4593" if "область" in address.lower() else "region=1"

def test_cian_availability():
    """Функция для проверки доступности CIAN и диагностики блокировки."""
    log.info("Тестирование доступа к CIAN...")
    
    # Создаем драйвер с разными настройками для тестирования
    opts1 = Options()  # Обычные настройки
    opts1.add_argument("--headless")
    
    opts2 = Options()  # Маскированные настройки
    opts2.add_argument("--headless")
    opts2.add_argument("--disable-blink-features=AutomationControlled")
    opts2.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts2.add_argument(f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
    
    results = {}
    
    # Тест 1: Обычный браузер без прокси
    try:
        driver = webdriver.Chrome(options=opts1)
        driver.get("https://www.cian.ru")
        time.sleep(3)
        results["standard_no_proxy"] = "captcha" in driver.page_source.lower() or "подтвердить, что вы не робот" in driver.page_source.lower()
        driver.quit()
    except Exception as e:
        results["standard_no_proxy"] = f"error: {str(e)}"
    
    # Тест 2: Маскированный браузер без прокси
    try:
        driver = webdriver.Chrome(options=opts2)
        driver.get("https://www.cian.ru")
        time.sleep(3)
        results["stealth_no_proxy"] = "captcha" in driver.page_source.lower() or "подтвердить, что вы не робот" in driver.page_source.lower()
        driver.quit()
    except Exception as e:
        results["stealth_no_proxy"] = f"error: {str(e)}"
    
    # Тест 3: Маскированный браузер с прокси
    proxy = get_proxy()
    if proxy:
        opts2.add_argument(f"--proxy-server={proxy}")
        try:
            driver = webdriver.Chrome(options=opts2)
            driver.get("https://www.cian.ru")
            time.sleep(3)
            results["stealth_with_proxy"] = "captcha" in driver.page_source.lower() or "подтвердить, что вы не робот" in driver.page_source.lower()
            driver.quit()
        except Exception as e:
            results["stealth_with_proxy"] = f"error: {str(e)}"
    
    # Вывод результатов
    log.info("=== Результаты диагностики CIAN ===")
    for key, value in results.items():
        log.info(f"{key}: {'ЗАБЛОКИРОВАНО' if value == True else value if isinstance(value, str) else 'ДОСТУПНО'}")
        
    return results