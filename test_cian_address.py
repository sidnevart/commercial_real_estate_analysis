import undetected_chromedriver as uc
import json
import re
import time
import logging
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

def setup_driver():
    """Настройка драйвера с обходом обнаружения"""
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(60)
    
    return driver

def extract_address_from_json(data, prefix=""):
    """Рекурсивный поиск адреса в структуре JSON"""
    results = {}
    
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else key
            
            # Ищем ключи, связанные с адресом
            if any(addr_key in key.lower() for addr_key in ["address", "location", "geo"]):
                if isinstance(value, str) and len(value) > 10:
                    results[path] = value
                
            # Проверим конкретные пути из adfoxOffer
            if key == "adfoxOffer" and isinstance(value, dict) and "response" in value:
                try:
                    unicom_address = value["response"]["data"]["unicomLinkParams"]["puid14"]
                    if isinstance(unicom_address, str) and len(unicom_address) > 10:
                        results[f"{path}.response.data.unicomLinkParams.puid14"] = unicom_address
                except (KeyError, TypeError):
                    pass
            
            # Рекурсивно обходим вложенные структуры
            if isinstance(value, (dict, list)):
                nested_results = extract_address_from_json(value, path)
                results.update(nested_results)
                
    elif isinstance(data, list):
        for i, item in enumerate(data):
            path = f"{prefix}[{i}]"
            nested_results = extract_address_from_json(item, path)
            results.update(nested_results)
    
    return results

def extract_address_from_dom(driver):
    """Извлечение адреса из DOM с помощью различных селекторов"""
    selectors = {
        "meta_og_description": "meta[property='og:description']",
        "data_name_geo": "[data-name='Geo']",
        "data_name_address": "[data-name='AddressContainer']",
        "geo_label": ".a10a3f92e9--geo--RgL1J",
        "underground": ".a10a3f92e9--underground--akzmo",
        "address_component": ".a10a3f92e9--address--ScQMM",
        "address_class": ".address--GbMIh",
        "desktop_address": ".desktop--address--bfmVy",
        "geo_address": ".geo-address--U0Sxb",
        "address": ".address"
    }
    
    results = {}
    
    for name, selector in selectors.items():
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                for i, element in enumerate(elements):
                    text = element.text.strip()
                    if text and len(text) > 10:
                        results[f"{name}_{i}"] = text
                        
                    # Для мета-тегов
                    if selector.startswith("meta"):
                        content = element.get_attribute("content")
                        if content and len(content) > 10:
                            results[f"{name}_content_{i}"] = content
        except Exception as e:
            logger.error(f"Ошибка при извлечении через селектор {name}: {e}")
    
    # JS-метод для поиска в DOM
    try:
        js_result = driver.execute_script("""
        (function() {
            const selectors = [
                '[data-name="Geo"]',
                '[data-name="AddressContainer"]',
                '[data-name="GeoLabel"]',
                '.a10a3f92e9--geo--RgL1J',
                '.a10a3f92e9--underground--akzmo',
                '.a10a3f92e9--address--ScQMM',
                '.address-unit',
                '.address--GbMIh',
                '.geo-address--U0Sxb',
                '.address',
                '.address__string'
            ];
            
            // Ищем элементы с адресами
            for (const selector of selectors) {
                const elements = document.querySelectorAll(selector);
                for (const el of elements) {
                    if (el && el.textContent && el.textContent.trim().length > 10) {
                        const text = el.textContent.trim();
                        if ((text.includes('Москва') || text.includes('область')) && 
                           (text.includes('район') || text.includes('улица') || 
                            text.includes('проспект') || text.includes('м.'))) {
                            return text;
                        }
                    }
                }
            }
            
            // Ищем элементы с метро и временем до метро
            const metroText = [];
            const metroSelectors = ['.underground-before-title--o4Px1', '.underground-station--iJzqA', '[data-name="MetroInfo"]', '.a10a3f92e9--underground--akzmo'];
            for (const selector of metroSelectors) {
                const elements = document.querySelectorAll(selector);
                for (const el of elements) {
                    if (el && el.textContent && el.textContent.includes('минут')) {
                        metroText.push(el.textContent.trim());
                        break;
                    }
                }
            }
            
            // Ищем элементы с адресами
            const addressSelectors = ['.a10a3f92e9--address--ScQMM', '.address--GbMIh', '.geo-address--U0Sxb', '.address', '[data-name="GeoLabel"]'];
            const addressText = [];
            for (const selector of addressSelectors) {
                const elements = document.querySelectorAll(selector);
                for (const el of elements) {
                    if (el && el.textContent && el.textContent.trim().length > 10) {
                        addressText.push(el.textContent.trim());
                        break;
                    }
                }
            }
            
            return { metro: metroText, address: addressText };
        })();
        """)
        
        if isinstance(js_result, dict):
            if "metro" in js_result and js_result["metro"]:
                results["js_metro"] = js_result["metro"]
            if "address" in js_result and js_result["address"]:
                results["js_address"] = js_result["address"]
        elif js_result:
            results["js_combined"] = js_result
    except Exception as e:
        logger.error(f"Ошибка при JS-извлечении: {e}")
        
    return results

def test_page_address(url):
    """Тестирует извлечение адреса с указанной страницы ЦИАН"""
    logger.info(f"Тестируем страницу: {url}")
    
    driver = setup_driver()
    
    try:
        # Открываем страницу
        driver.get(url)
        time.sleep(5)  # Даем время на загрузку
        
        page_source = driver.page_source
        
        # Сохраняем страницу для анализа
        with open("test_cian_page.html", "w", encoding="utf-8") as f:
            f.write(page_source)
        logger.info("Сохранена копия страницы в test_cian_page.html")
        
        # 1. Извлекаем все JSON-данные со страницы
        json_data = {}
        
        # frontend-offer-card
        script_match = re.search(r'window\._cianConfig\[\'frontend-offer-card\'\]\.concat\((.*?)\);', page_source)
        if script_match:
            try:
                json_text = script_match.group(1)
                config = json.loads(json_text)
                
                state_block = next((block for block in config if "key" in block and block["key"] == "defaultState"), None)
                if state_block:
                    json_data["frontend-offer-card"] = state_block["value"]
                    logger.info("✅ Извлечен JSON из 'frontend-offer-card'")
            except Exception as e:
                logger.error(f"❌ Ошибка при разборе JSON из 'frontend-offer-card': {e}")
        
        # _initialData
        alt_script_match = re.search(r'window\._initialData\s*=\s*({.*?});', page_source)
        if alt_script_match:
            try:
                json_text = alt_script_match.group(1)
                json_data["_initialData"] = json.loads(json_text)
                logger.info("✅ Извлечен JSON из '_initialData'")
            except Exception as e:
                logger.error(f"❌ Ошибка при разборе JSON из '_initialData': {e}")
        
        # _CIAN_COMPONENT_DATA_
        scripts = re.findall(r'window\._CIAN_COMPONENT_DATA_\s*=\s*({.*?});', page_source)
        if scripts:
            for i, script_text in enumerate(scripts):
                try:
                    json_data[f"_CIAN_COMPONENT_DATA_{i}"] = json.loads(script_text)
                    logger.info(f"✅ Извлечен JSON из '_CIAN_COMPONENT_DATA_{i}'")
                except Exception as e:
                    logger.error(f"❌ Ошибка при разборе JSON из '_CIAN_COMPONENT_DATA_{i}': {e}")
        
        # 2. Ищем адрес в JSON-данных
        address_from_json = {}
        for source, data in json_data.items():
            addresses = extract_address_from_json(data)
            for path, address in addresses.items():
                address_from_json[f"{source}:{path}"] = address
        
        # 3. Извлекаем адрес из DOM
        address_from_dom = extract_address_from_dom(driver)
        
        # 4. Выводим результаты
        logger.info("\n\n=== РЕЗУЛЬТАТЫ ИЗВЛЕЧЕНИЯ АДРЕСА ===")
        
        if address_from_json:
            logger.info("\n--- Адреса из JSON-данных ---")
            for path, address in address_from_json.items():
                logger.info(f"{path}: {address}")
        else:
            logger.info("⚠️ Адреса в JSON-данных не найдены")
        
        if address_from_dom:
            logger.info("\n--- Адреса из DOM ---")
            for selector, address in address_from_dom.items():
                logger.info(f"{selector}: {address}")
        else:
            logger.info("⚠️ Адреса в DOM не найдены")
        
        # 5. Проверяем adfoxOffer в parsing_torgi_and_cian.py
        adfox_result = None
        try:
            if "frontend-offer-card" in json_data and "adfoxOffer" in json_data["frontend-offer-card"]:
                adfox_result = json_data["frontend-offer-card"]["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                logger.info(f"\n✅ Метод из parsing_torgi_and_cian.py: {adfox_result}")
        except (KeyError, TypeError):
            logger.warning("⚠️ Не удалось извлечь адрес методом из parsing_torgi_and_cian.py")
        
    except Exception as e:
        logger.error(f"Ошибка при обработке страницы: {e}")
    finally:
        driver.quit()
        
    return address_from_json, address_from_dom, adfox_result

def test_search_page_addresses(search_url):
    """
    Тестовая функция для извлечения адресов объявлений со страницы поиска ЦИАН.
    
    Args:
        search_url (str): URL страницы поиска ЦИАН
    """
    import os
    
    logger.info(f"Тестируем страницу поиска: {search_url}")
    
    # Создаем директорию для результатов
    results_dir = "search_page_test_results"
    os.makedirs(results_dir, exist_ok=True)
    
    driver = setup_driver()
    
    try:
        # Открываем страницу поиска
        driver.get(search_url)
        time.sleep(5)  # Даем время на загрузку
        
        search_page_source = driver.page_source
        
        # Сохраняем копию страницы поиска
        with open(f"{results_dir}/search_page.html", "w", encoding="utf-8") as f:
            f.write(search_page_source)
        logger.info(f"Сохранена копия страницы поиска в {results_dir}/search_page.html")
        
        # Создаем файл для результатов
        results_file = f"{results_dir}/address_comparison.txt"
        with open(results_file, "w", encoding="utf-8") as f:
            f.write("Объявление | Адрес со страницы поиска | Адрес с полной страницы | Совпадение\n")
            f.write("-" * 120 + "\n")
        
        # Извлечение URL объявлений со страницы
        offer_urls = []
        
        # Метод 1: Через Selenium - поиск всех ссылок, содержащих commercial
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/commercial/']")
        for link in links:
            url = link.get_attribute("href")
            if url and '/commercial/' in url and url not in offer_urls:
                offer_urls.append(url)
        
        # Метод 2: Поиск через регулярные выражения в HTML
        patterns = [
            r'href="(https?://www\.cian\.ru/(?:rent|sale)/commercial/\d+/)"',
            r'href="(/(?:rent|sale)/commercial/\d+/)"',
            r'data-url="(https?://www\.cian\.ru/(?:rent|sale)/commercial/\d+/)"'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, search_page_source)
            for match in matches:
                url = match if match.startswith('http') else f"https://www.cian.ru{match}"
                if url not in offer_urls:
                    offer_urls.append(url)
        
        # Метод 3: Извлечение из JSON данных
        json_pattern = r'window\._cianConfig\[\'frontend-serp\'\]\.concat\((.*?)\);'
        script_match = re.search(json_pattern, search_page_source, re.DOTALL)
        if script_match:
            try:
                json_text = script_match.group(1)
                config = json.loads(json_text)
                
                offers_data_block = None
                for block in config:
                    if "key" in block and block["key"] == "defaultState" and "value" in block:
                        state = block["value"]
                        if "offersSerialized" in state and "results" in state["offersSerialized"]:
                            offers_data_block = state["offersSerialized"]["results"]
                            break
                
                if offers_data_block:
                    for offer in offers_data_block:
                        if "fullUrl" in offer and offer["fullUrl"] not in offer_urls:
                            offer_urls.append(offer["fullUrl"])
            except Exception as e:
                logger.error(f"Ошибка при извлечении URL из JSON данных: {e}")
        
        # Удаляем дубликаты и ограничиваем количество для теста
        offer_urls = list(set(offer_urls))[:10]  # Берем максимум 10 URL
        logger.info(f"Обнаружено {len(offer_urls)} уникальных URL объявлений")
        
        # Обрабатываем каждое объявление
        for i, url in enumerate(offer_urls, 1):
            logger.info(f"Обработка объявления {i}/{len(offer_urls)}: {url}")
            
            # Извлекаем адрес со страницы поиска через анализ JSON данных
            search_page_address = "Не найден"
            offer_id = url.split('/')[-2] if url.endswith('/') else url.split('/')[-1]
            
            try:
                # Пытаемся найти данные объявления в JSON поисковой страницы
                if script_match:
                    json_text = script_match.group(1)
                    config = json.loads(json_text)
                    
                    for block in config:
                        if "key" in block and block["key"] == "defaultState" and "value" in block:
                            state = block["value"]
                            if "offersSerialized" in state and "results" in state["offersSerialized"]:
                                for offer in state["offersSerialized"]["results"]:
                                    if str(offer.get("id", "")) == offer_id:
                                        # Пытаемся извлечь адрес из разных частей JSON
                                        if "geo" in offer and "userFormattedAddress" in offer["geo"]:
                                            search_page_address = offer["geo"]["userFormattedAddress"]
                                        elif "address" in offer:
                                            if isinstance(offer["address"], str):
                                                search_page_address = offer["address"]
                                            elif isinstance(offer["address"], dict) and "fullAddress" in offer["address"]:
                                                search_page_address = offer["address"]["fullAddress"]
                                        break
            except Exception as e:
                logger.error(f"Ошибка при извлечении адреса со страницы поиска для {url}: {e}")
            
            # Теперь открываем полную страницу объявления для сравнения
            full_page_address = "Не найден"
            try:
                driver.get(url)
                time.sleep(5)  # Даем время на загрузку
                
                offer_page = driver.page_source
                
                # Метод 1: Извлечение через adfoxOffer
                offer_script_match = re.search(r'window\._cianConfig\[\'frontend-offer-card\'\]\.concat\((.*?)\);', offer_page)
                if offer_script_match:
                    offer_json_text = offer_script_match.group(1)
                    offer_config = json.loads(offer_json_text)
                    
                    offer_state = next((block["value"] for block in offer_config if "key" in block and block["key"] == "defaultState"), None)
                    if offer_state and "adfoxOffer" in offer_state:
                        try:
                            full_page_address = offer_state["adfoxOffer"]["response"]["data"]["unicomLinkParams"]["puid14"]
                        except (KeyError, TypeError):
                            pass
                
                # Метод 2: DOM извлечение если первый метод не сработал
                if full_page_address == "Не найден":
                    address_js = """
                    (function() {
                        function cleanAddress(text) {
                            return text ? text.replace(/ на карте/g, '').replace(/на карте/g, '').trim() : '';
                        }
                        
                        const selectors = [
                            '[data-name="Geo"]',
                            '[data-name="AddressContainer"]',
                            '.a10a3f92e9--geo--RgL1J',
                            '.a10a3f92e9--address--ScQMM'
                        ];
                        
                        for (const selector of selectors) {
                            const elements = document.querySelectorAll(selector);
                            for (const el of elements) {
                                if (el && el.textContent && el.textContent.trim().length > 10) {
                                    return cleanAddress(el.textContent.trim());
                                }
                            }
                        }
                        return 'Не найден';
                    })();
                    """
                    dom_result = driver.execute_script(address_js)
                    if dom_result != "Не найден":
                        full_page_address = dom_result
                
                # Оцениваем совпадение адресов
                if search_page_address != "Не найден" and full_page_address != "Не найден":
                    # Нормализация для сравнения
                    norm_search = re.sub(r'\s+', ' ', search_page_address.lower().strip())
                    norm_full = re.sub(r'\s+', ' ', full_page_address.lower().strip())
                    
                    # Проверяем совпадение
                    if norm_search == norm_full:
                        match = "ПОЛНОЕ совпадение"
                    elif norm_search in norm_full or norm_full in norm_search:
                        match = "ЧАСТИЧНОЕ совпадение"
                    else:
                        match = "НЕТ совпадения"
                else:
                    match = "Невозможно сравнить"
                
                # Записываем результат
                with open(results_file, "a", encoding="utf-8") as f:
                    f.write(f"{url} | {search_page_address} | {full_page_address} | {match}\n")
                
            except Exception as e:
                logger.error(f"Ошибка при обработке полной страницы {url}: {e}")
                with open(results_file, "a", encoding="utf-8") as f:
                    f.write(f"{url} | {search_page_address} | ОШИБКА: {str(e)} | ОШИБКА\n")
        
        logger.info(f"Результаты сохранены в {results_file}")
        
    except Exception as e:
        logger.error(f"Общая ошибка при обработке страницы поиска: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    # Список тестовых URL отдельных объявлений для проверки
    test_detail_urls = [
        "https://www.cian.ru/rent/commercial/311622750/",
        "https://www.cian.ru/sale/commercial/313633947/"
    ]
    
    # Список тестовых URL поисковых страниц
    test_search_urls = [
        "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=offices&region=1",
        "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2&offer_type=offices&region=1"
    ]
    
    # Тестирование отдельных страниц
    for url in test_detail_urls:
        try:
            json_results, dom_results, adfox_result = test_page_address(url)
            
            # Записываем результаты в файл для анализа
            with open(f"address_results_{url.split('/')[-2]}.json", "w", encoding="utf-8") as f:
                json.dump({
                    "url": url,
                    "json_results": json_results,
                    "dom_results": dom_results,
                    "adfox_result": adfox_result
                }, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Результаты сохранены в address_results_{url.split('/')[-2]}.json")
            logger.info("-" * 80)
            
        except Exception as e:
            logger.error(f"Ошибка при тестировании {url}: {e}")
            
        time.sleep(5)  # Пауза между запросами
    
    # Тестирование страниц поиска
    for search_url in test_search_urls:
        test_search_page_addresses(search_url)
        time.sleep(10)  # Пауза между тестами поисковых страниц