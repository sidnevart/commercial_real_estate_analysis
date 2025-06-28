"""
Тестовый скрипт для проверки форматирования Google Sheets таблиц
"""

import logging
import sys
from pathlib import Path

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logger = logging.getLogger(__name__)

def test_formatting():
    """Тестирует применение форматирования к существующим таблицам"""
    
    print("🧪 ТЕСТИРОВАНИЕ ФОРМАТИРОВАНИЯ GOOGLE SHEETS")
    print("=" * 60)
    
    try:
        # Импортируем функцию форматирования
        from parser.format_table import apply_all_formatting
        logger.info("✅ Успешно импортирована функция apply_all_formatting")
        
        # Применяем форматирование
        logger.info("🎨 Запуск форматирования существующих таблиц...")
        success = apply_all_formatting()
        
        if success:
            print("\n" + "="*60)
            print("🎉 ТЕСТИРОВАНИЕ УСПЕШНО ЗАВЕРШЕНО!")
            print("="*60)
            print("✅ Все таблицы отформатированы:")
            print("   • lots_all - основная таблица лотов")
            print("   • cian_sale_all - объявления о продаже")
            print("   • cian_rent_all - объявления об аренде")
            print("\n🔍 Проверьте таблицы в Google Sheets:")
            print("   • Числа с разрядностью (1,234,567₽)")
            print("   • Зеленые ячейки для доходности >= 20%")
            print("   • Зеленые ячейки для капитализации >= 15%")
            print("="*60)
            return True
        else:
            print("\n" + "="*60)
            print("⚠️ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО С ПРЕДУПРЕЖДЕНИЯМИ")
            print("="*60)
            print("❌ Некоторые таблицы не удалось отформатировать")
            print("📋 Проверьте логи выше для деталей")
            print("="*60)
            return False
            
    except ImportError as e:
        logger.error(f"❌ Ошибка импорта: {e}")
        print(f"\n❌ Не удалось импортировать format_sheets.py")
        print(f"📁 Убедитесь, что файл format_sheets.py находится в корне проекта")
        return False
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        print(f"\n❌ Критическая ошибка: {e}")
        print(f"📋 Проверьте конфигурацию Google Sheets API")
        return False

def check_dependencies():
    """Проверяет наличие всех необходимых зависимостей"""
    
    print("🔍 ПРОВЕРКА ЗАВИСИМОСТЕЙ")
    print("-" * 40)
    
    dependencies = [
        ("googleapiclient", "Google Sheets API"),
        ("google.oauth2", "Google Auth"),
        ("parser.config", "Конфигурация проекта")
    ]
    
    all_ok = True
    
    for module, description in dependencies:
        try:
            __import__(module)
            print(f"✅ {description}")
        except ImportError as e:
            print(f"❌ {description}: {e}")
            all_ok = False
    
    print("-" * 40)
    
    if all_ok:
        print("✅ Все зависимости доступны")
    else:
        print("❌ Есть проблемы с зависимостями")
    
    return all_ok

def main():
    """Основная функция тестирования"""
    
    print("🚀 ЗАПУСК ТЕСТИРОВАНИЯ ФОРМАТИРОВАНИЯ")
    print("=" * 60)
    
    # 1. Проверяем зависимости
    if not check_dependencies():
        print("\n❌ Остановка из-за проблем с зависимостями")
        return False
    
    print()
    
    # 2. Тестируем форматирование
    success = test_formatting()
    
    # 3. Итоговый результат
    print(f"\n🏁 ИТОГ ТЕСТИРОВАНИЯ: {'УСПЕХ' if success else 'НЕУДАЧА'}")
    
    return success

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Тестирование прервано пользователем")
    except Exception as e:
        print(f"\n💥 Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()