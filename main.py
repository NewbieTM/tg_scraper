import os
import sys
import asyncio
from datetime import datetime
from dotenv import load_dotenv

# Импорт наших модулей
from core.client import TelegramClientManager
from core.scraper import TelegramScraper
from core.publisher import ChannelPublisher
from core.db_manager import DatabaseManager

load_dotenv()

# Проверка обязательных переменных окружения
required_env = [
    'API_ID', 'API_HASH', 'PHONE',
    'CHANNELS', 'MY_CHANNEL',
    'MEDIA_SAVE_PATH',
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'
]
missing = [var for var in required_env if not os.getenv(var)]
if missing:
    print(f"❌ Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
    sys.exit(1)

CONFIG = {
    'api_id': int(os.getenv('API_ID')),
    'api_hash': os.getenv('API_HASH'),
    'phone': os.getenv('PHONE'),
    'channels': [x.strip() for x in os.getenv('CHANNELS').split(',') if x.strip()],
    'parse_interval': int(os.getenv('PARSE_INTERVAL', 300)),       # По умолчанию 300 с (5 минут)
    'parse_limit': int(os.getenv('PARSE_LIMIT', 5)),
    'session_file': os.getenv('SESSION_FILE', 'session.session'),
    'target_channel': os.getenv('MY_CHANNEL'),
    'media_path': os.getenv('MEDIA_SAVE_PATH'),
    'max_media': int(os.getenv('MAX_MEDIA_PER_POST', 10)),
    'max_caption': int(os.getenv('MAX_CAPTION_LENGTH', 1000)),
    'publish_delay': int(os.getenv('PUBLISH_DELAY', 2))
}


async def main():
    # 1) Создаём директорию для медиа (если не существует)
    os.makedirs(CONFIG['media_path'], exist_ok=True)

    # 2) Инициализируем Telegram-клиент
    client_manager = TelegramClientManager(
        session_file=CONFIG['session_file'],
        api_id=CONFIG['api_id'],
        api_hash=CONFIG['api_hash'],
        phone=CONFIG['phone']
    )
    try:
        tg_client = await client_manager.start()
    except Exception as e:
        print(f"❌ Не удалось запустить TelegramClientManager: {e}")
        return

    # 3) Инициализируем базу данных
    db_manager = DatabaseManager()
    try:
        await db_manager.initialize()
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        await tg_client.disconnect()
        return

    # 4) Создаём скрапер и паблишер
    scraper = TelegramScraper(
        client=tg_client,
        db_manager=db_manager,
        media_path=CONFIG['media_path']
    )
    publisher = ChannelPublisher(
        client=tg_client,
        target_channel=CONFIG['target_channel'],
        max_media=CONFIG['max_media'],
        max_caption=CONFIG['max_caption'],
        delay=CONFIG['publish_delay']
    )
    try:
        await publisher.initialize()
    except Exception as e:
        print(f"❌ Ошибка инициализации ChannelPublisher: {e}")
        await db_manager.close()
        await tg_client.disconnect()
        return

    print("🚀 Скрапер запущен. Для остановки нажмите Ctrl+C")

    try:
        while True:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n⏳ Начало парсинга: {now_str}")

            all_posts = []
            for channel_username in CONFIG['channels']:
                print(f"🔍 Парсинг канала `{channel_username}`...")
                try:
                    new_posts = await scraper.scrape_channel(channel_username, limit=CONFIG['parse_limit'])
                except Exception as e:
                    print(f"  ❌ Ошибка при scrape_channel({channel_username}): {e}")
                    continue

                if new_posts:
                    print(f"  ✅ Найдено {len(new_posts)} новых постов")
                    all_posts.extend(new_posts)
                else:
                    print("  ℹ️ Новых постов нет.")
                # Небольшая задержка между запросами, чтобы не «флудить» Telegram API
                await asyncio.sleep(2)

            # Если есть новые посты, публикуем
            if all_posts:
                print(f"\n🔄 Начало публикации {len(all_posts)} постов...")
                for post in all_posts:
                    attempts = 3
                    while attempts > 0:
                        success = await publisher.publish(post)
                        if success:
                            print(f"  ✅ Успешно опубликован пост ID={post['id']} из канала `{post['channel']}`")

                            # Удаляем медиа после удачной публикации
                            await publisher.clean_media(post)

                            # Помечаем в БД как «опубликовано»
                            await db_manager.mark_post_published(post_id=post['id'], channel=post['channel'])
                            break

                        attempts -= 1
                        if attempts > 0:
                            print(f"  ⚠️ Ошибка при публикации поста {post['id']}. Попыток осталось: {attempts}. Повтор через 10 сек.")
                            await asyncio.sleep(10)
                        else:
                            print(f"  ❌ Не удалось опубликовать пост {post['id']} после 3 попыток.")
                    # Задержка между публикациями (чтобы не получить FloodWaitError сразу)
                    await asyncio.sleep(CONFIG['publish_delay'])

            # Ждём до следующего цикла
            print(f"⏰ Ждём {CONFIG['parse_interval']} секунд до следующего парсинга...\n")
            await asyncio.sleep(CONFIG['parse_interval'])

    except KeyboardInterrupt:
        print("\n🛑 Выключение скрапера по Ctrl+C...")
    except Exception as e:
        print(f"\n❌ Критическая ошибка в основном цикле: {e}")
    finally:
        await db_manager.close()
        await tg_client.disconnect()
        print("🔌 Все соединения закрыты, программа завершена.")


if __name__ == '__main__':
    # Для Windows Event Loop
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
