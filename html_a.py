#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Улучшенный извлекатель площади ЦИАН с фокусом на заголовки и валидацию
"""

import re
import json
import logging
from bs4 import BeautifulSoup

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
    
    # 1. Из переданного offer_info
    if offer_info:
        try:
            json_area = offer_info.get("offerData", {}).get("offer", {}).get("totalArea")
            if json_area:
                area = float(json_area)
                logger.info(f"Площадь из offer_info: {area}")
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

def test_area_extraction():
    """Тестирует извлечение площади на сохраненных файлах"""
    import glob
    
    html_files = glob.glob("cian_page_*.html")
    
    for html_file in html_files:
        print(f"\n{'='*60}")
        print(f"Тестирование файла: {html_file}")
        print(f"{'='*60}")
        
        try:
            with open(html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Тестируем извлечение площади
            area = extract_area_smart(soup)
            
            # Также показываем заголовки для сравнения
            title = soup.find('title')
            if title:
                print(f"Заголовок: {title.get_text()}")
            
            if area:
                print(f"✅ Извлеченная площадь: {area} м²")
            else:
                print("❌ Площадь не извлечена")
            
        except Exception as e:
            print(f"Ошибка при обработке {html_file}: {e}")

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Запуск тестирования
    test_area_extraction()