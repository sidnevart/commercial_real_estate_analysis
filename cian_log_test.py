#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обновленный тестовый скрипт для отладки парсера ЦИАН
С улучшенным механизмом извлечения площади из заголовков
"""

import requests
import json
import re
import logging
from bs4 import BeautifulSoup
import time
import glob

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cian_parser_enhanced_test.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_area_smart(soup, offer_info=None):
    """
    Умное извлечение площади с приоритетом заголовков и валидацией
    """
    logger.info("=== SMART AREA EXTRACTION ===")
    
    # 1. ПРИОРИТЕТ: Площадь из заголовков (title, og:title)
    title_area = extract_area_from_titles(soup)
    if title_area:
        logger.info(f"✅ Площадь из заголовков: {title_area} м²")
        return title_area
    
    # 2. Площадь из JSON структурированных данных (с валидацией)
    json_area = extract_area_from_json(soup, offer_info)
    if json_area and validate_area(json_area, title_area):
        logger.info(f"✅ Площадь из JSON (валидна): {json_area} м²")
        return json_area
    
    # 3. Площадь из специфических полей (только основные)
    field_area = extract_area_from_specific_fields(soup)
    if field_area and validate_area(field_area, title_area):
        logger.info(f"✅ Площадь из полей (валидна): {field_area} м²")
        return field_area
    
    logger.warning("❌ Площадь не найдена или не валидна")
    return None

def extract_area_from_titles(soup):
    """Извлекает площадь только из заголовков страницы"""
    logger.info("--- Поиск площади в заголовках ---")
    
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
            
        logger.debug(f"Анализ {source_name}: {text[:100]}...")
        
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
                        logger.info(f"Найден диапазон в {source_name}: {area1}-{area2}, выбрана {area}")
                        return area
                    except ValueError:
                        continue
                else:
                    # Одиночное значение
                    try:
                        area = float(match.replace(',', '.') if isinstance(match, str) else match)
                        if 50 <= area <= 5000:  # Разумные пределы
                            logger.info(f"Найдена площадь в {source_name}: {area}")
                            return area
                    except (ValueError, AttributeError):
                        continue
    
    logger.info("Площадь в заголовках не найдена")
    return None

def extract_area_from_json(soup, offer_info):
    """Извлекает площадь из JSON данных с осторожностью"""
    logger.info("--- Поиск площади в JSON ---")
    
    # Из JSON-LD (очень осторожно)
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
                            logger.info(f"Площадь из JSON-LD[{i}].{field}: {area}")
                            return area
        except (json.JSONDecodeError, ValueError, TypeError):
            continue
    
    logger.info("Площадь в JSON не найдена")
    return None

def extract_area_from_specific_fields(soup):
    """Извлекает площадь только из специфических полей площади"""
    logger.info("--- Поиск площади в специфических полях ---")
    
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
            logger.debug(f"Проверка {selector}: {text}")
            
            # Ищем числовое значение площади
            area_match = re.search(r'(\d+(?:[,\.]\d+)?)', text)
            if area_match:
                try:
                    area = float(area_match.group(1).replace(',', '.'))
                    if 50 <= area <= 5000:  # Разумные пределы
                        logger.info(f"Площадь из поля {selector}: {area}")
                        return area
                except ValueError:
                    continue
    
    logger.info("Площадь в специфических полях не найдена")
    return None

def validate_area(candidate_area, reference_area=None):
    """Валидирует найденную площадь"""
    if not candidate_area:
        return False
    
    # Базовая валидация - разумные пределы
    if not (50 <= candidate_area <= 5000):
        logger.warning(f"Площадь {candidate_area} вне разумных пределов")
        return False
    
    # Если есть эталонная площадь из заголовка, проверяем совместимость
    if reference_area:
        # Разрешаем отклонение до 20%
        diff_percent = abs(candidate_area - reference_area) / reference_area * 100
        if diff_percent > 20:
            logger.warning(f"Площадь {candidate_area} слишком отличается от заголовка {reference_area} ({diff_percent:.1f}%)")
            return False
    
    return True

class CianParserTesterEnhanced:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def get_page_content(self, url):
        """Получает содержимое страницы"""
        logger.info(f"Получение страницы: {url}")
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            logger.info(f"Статус ответа: {response.status_code}")
            return response.text
        except Exception as e:
            logger.error(f"Ошибка при получении страницы {url}: {e}")
            return None

    def save_html_to_file(self, html_content, url):
        """Сохраняет HTML в файл для анализа"""
        offer_id = re.search(r'/(\d+)/?$', url)
        if offer_id:
            filename = f"cian_page_{offer_id.group(1)}.html"
        else:
            filename = f"cian_page_{int(time.time())}.html"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"HTML сохранен в файл: {filename}")
            return filename
        except Exception as e:
            logger.error(f"Ошибка при сохранении HTML: {e}")
            return None

    def extract_basic_info(self, soup):
        """Извлекает базовую информацию об объявлении"""
        info = {}
        
        # Заголовок
        title_tag = soup.find('title')
        if title_tag:
            info['title'] = title_tag.get_text().strip()
        
        # og:title
        og_title = soup.find('meta', {'property': 'og:title'})
        if og_title:
            info['og_title'] = og_title.get('content', '').strip()
        
        # description
        description = soup.find('meta', {'name': 'description'})
        if description:
            info['description'] = description.get('content', '').strip()
        
        return info

    def test_single_url(self, url):
        """Тестирует парсинг одного URL с улучшенным извлечением площади"""
        logger.info(f"\n{'='*80}")
        logger.info(f"ТЕСТИРОВАНИЕ URL: {url}")
        logger.info(f"{'='*80}")
        
        # Получаем содержимое страницы
        html_content = self.get_page_content(url)
        if not html_content:
            logger.error(f"Не удалось получить содержимое страницы: {url}")
            return None
        
        # Сохраняем HTML в файл
        html_file = self.save_html_to_file(html_content, url)
        
        # Парсим HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Извлекаем базовую информацию
        basic_info = self.extract_basic_info(soup)
        
        # Извлекаем площадь умным способом
        area = extract_area_smart(soup)
        
        # Результат
        result = {
            'url': url,
            'html_file': html_file,
            'title': basic_info.get('title', 'Не найден'),
            'og_title': basic_info.get('og_title', 'Не найден'),
            'description': basic_info.get('description', 'Не найден'),
            'extracted_area': area,
            'area_status': '✅ Найдена' if area else '❌ Не найдена'
        }
        
        # Выводим сводку
        logger.info(f"Заголовок: {basic_info.get('title', 'Не найден')}")
        logger.info(f"Площадь: {area} м²" if area else "Площадь: НЕ НАЙДЕНА")
        
        return result

    def run_extended_tests(self):
        """Запускает тесты для расширенного списка URL"""
        test_urls = [
            "https://podolsk.cian.ru/rent/commercial/316990913/",
            "https://podolsk.cian.ru/rent/commercial/316435827/",
            "https://podolsk.cian.ru/rent/commercial/318680980/",
            "https://podolsk.cian.ru/rent/commercial/317607741/",
            "https://podolsk.cian.ru/rent/commercial/314550323/",
            "https://www.cian.ru/rent/commercial/318628444/",
            "https://www.cian.ru/rent/commercial/318105784/",
            "https://www.cian.ru/rent/commercial/319058946/",
            "https://www.cian.ru/rent/commercial/317597811/",
            "https://www.cian.ru/rent/commercial/318393830/",
            "https://www.cian.ru/rent/commercial/318393596/",
            "https://www.cian.ru/rent/commercial/317411914/",
            "https://www.cian.ru/rent/commercial/318151255/",
            "https://www.cian.ru/rent/commercial/317566521/",
            "https://www.cian.ru/rent/commercial/319017000/",
            "https://www.cian.ru/rent/commercial/318867375/",
            "https://www.cian.ru/rent/commercial/318414504/",
            "https://www.cian.ru/rent/commercial/317705461/",
            "https://www.cian.ru/rent/commercial/318809878/",
            "https://www.cian.ru/rent/commercial/317152577/",
            "https://www.cian.ru/rent/commercial/317601590/",
            "https://www.cian.ru/rent/commercial/316267479/",
            "https://www.cian.ru/rent/commercial/318305882/",
            "https://www.cian.ru/rent/commercial/318397644/",
            "https://www.cian.ru/rent/commercial/318192920/",
            "https://www.cian.ru/rent/commercial/318607665/",
            "https://www.cian.ru/rent/commercial/318525258/",
            "https://www.cian.ru/rent/commercial/318800325/",
            "https://www.cian.ru/rent/commercial/317567411/",
            "https://www.cian.ru/rent/commercial/195336746/",
            "https://www.cian.ru/rent/commercial/309775733/",
            "https://www.cian.ru/rent/commercial/318210114/",
            "https://www.cian.ru/rent/commercial/312141301/",
            "https://www.cian.ru/rent/commercial/312372808/",
            "https://www.cian.ru/rent/commercial/312369263/",
            "https://www.cian.ru/rent/commercial/299090917/",
            "https://www.cian.ru/rent/commercial/318796130/",
            "https://www.cian.ru/rent/commercial/318320602/",
            "https://www.cian.ru/rent/commercial/318111750/",
            "https://www.cian.ru/rent/commercial/317150829/",
            "https://www.cian.ru/rent/commercial/315799900/",
            "https://www.cian.ru/rent/commercial/311298878/",
            "https://www.cian.ru/rent/commercial/318564155/",
            "https://www.cian.ru/rent/commercial/318729061/",
            "https://www.cian.ru/rent/commercial/314632943/",
            "https://www.cian.ru/rent/commercial/312464420/",
            "https://www.cian.ru/rent/commercial/308893578/",
            "https://www.cian.ru/rent/commercial/318279546/",
        ]
        
        results = []
        successful = 0
        failed = 0
        
        for i, url in enumerate(test_urls, 1):
            try:
                logger.info(f"\n📍 Прогресс: {i}/{len(test_urls)} URL")
                result = self.test_single_url(url)
                if result:
                    results.append(result)
                    if result['extracted_area']:
                        successful += 1
                    else:
                        failed += 1
                
                # Пауза между запросами
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Ошибка при тестировании {url}: {e}")
                failed += 1
        
        # Сохраняем результаты
        try:
            with open('enhanced_test_results.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info("Результаты сохранены в enhanced_test_results.json")
        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов: {e}")
        
        # Статистика
        print(f"\n{'='*60}")
        print(f"📊 СТАТИСТИКА ТЕСТИРОВАНИЯ")
        print(f"{'='*60}")
        print(f"Всего URL: {len(test_urls)}")
        print(f"✅ Площадь найдена: {successful}")
        print(f"❌ Площадь НЕ найдена: {failed}")
        print(f"📈 Успешность: {successful/len(test_urls)*100:.1f}%")
        
        # Показываем примеры успешных извлечений
        success_examples = [r for r in results if r['extracted_area']][:5]
        if success_examples:
            print(f"\n✅ Примеры успешных извлечений:")
            for example in success_examples:
                print(f"  {example['extracted_area']} м² - {example['url']}")
        
        # Показываем примеры неуспешных
        fail_examples = [r for r in results if not r['extracted_area']][:5]
        if fail_examples:
            print(f"\n❌ Примеры неуспешных извлечений:")
            for example in fail_examples:
                print(f"  НЕТ ПЛОЩАДИ - {example['url']}")
        
        return results

def test_saved_files():
    """Тестирует извлечение площади из уже сохраненных HTML файлов"""
    print(f"\n{'='*60}")
    print(f"🔍 ТЕСТИРОВАНИЕ СОХРАНЕННЫХ HTML ФАЙЛОВ")
    print(f"{'='*60}")
    
    html_files = glob.glob("cian_page_*.html")
    if not html_files:
        print("Не найдено сохраненных HTML файлов cian_page_*.html")
        return
    
    results = []
    successful = 0
    
    for html_file in html_files[:10]:  # Тестируем первые 10 файлов
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Извлекаем заголовок
            title = soup.find('title')
            title_text = title.get_text() if title else "Не найден"
            
            # Извлекаем площадь
            area = extract_area_smart(soup)
            
            result = {
                'file': html_file,
                'title': title_text,
                'area': area,
                'status': '✅' if area else '❌'
            }
            results.append(result)
            
            if area:
                successful += 1
                print(f"✅ {html_file}: {area} м²")
            else:
                print(f"❌ {html_file}: площадь не найдена")
            
        except Exception as e:
            print(f"❌ Ошибка при обработке {html_file}: {e}")
    
    print(f"\nУспешность на сохраненных файлах: {successful}/{len(results)} ({successful/len(results)*100:.1f}%)")
    return results

if __name__ == "__main__":
    print("🚀 Запуск расширенного тестирования парсера ЦИАН...")
    
    # Сначала тестируем уже сохраненные файлы
    test_saved_files()
    
    # Затем запускаем тестирование новых URL
    tester = CianParserTesterEnhanced()
    results = tester.run_extended_tests()
    
    print(f"\n🎯 Тестирование завершено!")
    print("📁 Проверьте файлы:")
    print("  - cian_parser_enhanced_test.log - подробные логи")
    print("  - enhanced_test_results.json - результаты в JSON")
    print("  - cian_page_*.html - сохраненные HTML страницы")