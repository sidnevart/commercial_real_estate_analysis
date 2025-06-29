#!/usr/bin/env python3
"""
Диагностика проблемы с подписчиками
"""
import os
import json
import asyncio
from bot.bot_service import bot_service

def diagnose_subscribers():
    """Диагностирует проблему с подписчиками"""
    print("🔍 ДИАГНОСТИКА ПОДПИСЧИКОВ")
    print("=" * 50)
    
    # 1. Проверяем файл подписчиков
    subscribers_file = "bot_data/subscribers.json"
    print(f"📁 Проверка файла: {subscribers_file}")
    
    if os.path.exists(subscribers_file):
        print("✅ Файл существует")
        try:
            with open(subscribers_file, 'r') as f:
                subscribers_data = json.load(f)
            print(f"📋 Содержимое файла: {subscribers_data}")
            print(f"👥 Количество подписчиков в файле: {len(subscribers_data)}")
        except Exception as e:
            print(f"❌ Ошибка чтения файла: {e}")
    else:
        print("❌ Файл не существует")
        print(f"💡 Создайте файл с содержимым: [764315256]")
    
    # 2. Проверяем инициализацию бота
    print(f"\n🤖 Проверка инициализации бота:")
    try:
        bot_service.initialize()
        print("✅ Бот инициализирован")
        
        if hasattr(bot_service, 'bot'):
            print("✅ Объект бота создан")
            
            if hasattr(bot_service.bot, 'subscribers'):
                subscribers = bot_service.bot.subscribers
                print(f"👥 Подписчиков в боте: {len(subscribers)}")
                print(f"📋 Список подписчиков: {list(subscribers)}")
                
                if len(subscribers) == 0:
                    print("❌ ПРОБЛЕМА: Подписчики не загружены!")
                    print("💡 Попробуем загрузить асинхронно...")
                    return False  # Вернем False для запуска асинхронной загрузки
                else:
                    print("✅ Подписчики загружены корректно")
                    return True
            else:
                print("❌ У бота нет атрибута subscribers")
        else:
            print("❌ Объект бота не создан")
    except Exception as e:
        print(f"❌ Ошибка инициализации: {e}")
    
    # 3. Проверяем текущую директорию
    print(f"\n📂 Текущая директория: {os.getcwd()}")
    print(f"📁 Содержимое bot_data/:")
    if os.path.exists("bot_data"):
        for file in os.listdir("bot_data"):
            print(f"   • {file}")
    else:
        print("❌ Директория bot_data не существует")
    
    return False

async def async_diagnose():
    """Асинхронная диагностика с загрузкой подписчиков"""
    print("\n🔄 АСИНХРОННАЯ ДИАГНОСТИКА:")
    
    try:
        bot_service.initialize()
        
        if hasattr(bot_service, 'bot') and hasattr(bot_service.bot, '_load_subscribers'):
            print("📥 Принудительная загрузка подписчиков...")
            await bot_service.bot._load_subscribers()
            
            subscribers = bot_service.bot.subscribers
            print(f"✅ После загрузки: {len(subscribers)} подписчиков")
            print(f"📋 Список: {list(subscribers)}")
            
            return len(subscribers) > 0
        else:
            print("❌ Метод _load_subscribers не найден")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка асинхронной диагностики: {e}")
        return False

async def fix_subscribers():
    """Исправляет проблему с подписчиками"""
    print("\n🔧 ИСПРАВЛЕНИЕ ПРОБЛЕМЫ С ПОДПИСЧИКАМИ")
    print("=" * 50)
    
    # 1. Создаем директорию если нужно
    os.makedirs("bot_data", exist_ok=True)
    
    # 2. Создаем/обновляем файл подписчиков
    subscribers_file = "bot_data/subscribers.json"
    subscribers_data = [764315256]  # Ваш chat_id
    
    with open(subscribers_file, 'w') as f:
        json.dump(subscribers_data, f)
    print(f"✅ Файл {subscribers_file} создан/обновлен")
    
    # 3. Инициализируем бота заново
    bot_service.initialize()
    
    # 4. Принудительно загружаем подписчиков
    if hasattr(bot_service, 'bot'):
        await bot_service.bot._load_subscribers()
        print(f"✅ Подписчики загружены: {len(bot_service.bot.subscribers)}")
        return len(bot_service.bot.subscribers) > 0
    
    return False

async def main():
    """Главная функция"""
    # Сначала синхронная диагностика
    sync_result = diagnose_subscribers()
    
    if not sync_result:
        # Если проблема, пробуем асинхронную диагностику
        async_result = await async_diagnose()
        
        if not async_result:
            # Если всё ещё проблема, исправляем
            print("\n🚨 Найдены проблемы, пытаемся исправить...")
            fix_result = await fix_subscribers()
            
            if fix_result:
                print("\n🎉 Проблема исправлена! Теперь запустите основной тест.")
            else:
                print("\n❌ Проблема не исправлена. Проверьте настройки.")
        else:
            print("\n✅ Подписчики загружены успешно!")
    else:
        print("\n✅ Всё работает корректно!")

if __name__ == "__main__":
    asyncio.run(main())