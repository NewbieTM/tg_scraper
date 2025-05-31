import os
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv

from core.client import TelegramClientManager
from core.scraper import TelegramScraper
from core.storage import DataManager
from core.vector_db import VectorDatabase
from core.cleaner import DataCleaner
from core.publisher import ChannelPublisher

load_dotenv()

CONFIG = {
    'api_id': int(os.getenv('API_ID')),
    'api_hash': os.getenv('API_HASH'),
    'phone': os.getenv('PHONE'),
    'channels': [x.strip() for x in os.getenv('CHANNELS', '').split(',') if x.strip()],
    'parse_interval': int(os.getenv('PARSE_INTERVAL', 300)),  # по умолчанию 5 минут
    'session_file': os.getenv('SESSION_FILE', 'session'),
    'vector_db': os.getenv('VECTOR_DB_FILE', 'db/vector_db.pkl'),
    'similarity': float(os.getenv('SIMILARITY_THRESHOLD', 0.85)),
    'target_channel': os.getenv('MY_CHANNEL'),
    'retention_days': int(os.getenv('DATA_RETENTION_DAYS', 7)),
    'media_path': os.getenv('MEDIA_SAVE_PATH', 'media')
}


async def main():
    # Создаём необходимые папки
    os.makedirs('db', exist_ok=True)
    os.makedirs(CONFIG['media_path'], exist_ok=True)

    # Инициализируем Telegram-клиент
    client_manager = TelegramClientManager(
        CONFIG['session_file'],
        CONFIG['api_id'],
        CONFIG['api_hash'],
        CONFIG['phone']
    )
    tg_client = await client_manager.start()
    print("✅ Успешный вход в Telegram")

    # Инициализируем менеджер хранения, векторную БД, скрапер, паблишер, клинер
    storage = DataManager(
        output_file='db/posts.json',
        last_dates_file='db/last_dates.json'
    )
    vector_db = VectorDatabase(
        threshold=CONFIG['similarity'],
        db_file=CONFIG['vector_db']
    )
    scraper = TelegramScraper(tg_client, vector_db, storage)
    publisher = ChannelPublisher(tg_client, CONFIG['target_channel'], CONFIG['media_path'])
    cleaner = DataCleaner(vector_db, storage, CONFIG['retention_days'], CONFIG['media_path'])

    await publisher.initialize()
    last_clean = datetime.now() - timedelta(days=1)  # чтобы чистка сработала в первую же ночь

    print("🚀 Скрапер запущен. Для остановки Ctrl+C")

    try:
        while True:
            now = datetime.now()

            # Ежедневная очистка в 3:00 (если с последней прошло ≥24 часа)
            if now.hour == 3 and (now - last_clean).days >= 1:
                print("🧹 Начинаем ежедневную очистку данных...")
                await cleaner.clean_all()
                last_clean = now
                print("✅ Очистка завершена")

            print(f"\n⏳ Начало парсинга в {now.strftime('%Y-%m-%d %H:%M:%S')}")
            all_posts = []

            # Проходим по списку каналов
            for channel in CONFIG['channels']:
                print(f"🔍 Парсинг канала: {channel}")
                try:
                    new_posts = await scraper.scrape_channel(channel)
                    if new_posts:
                        print(f"   Найдено новых постов: {len(new_posts)}")
                        all_posts.extend(new_posts)
                    else:
                        print("   Новых постов не обнаружено")
                except Exception as e:
                    print(f"   Ошибка при парсинге канала {channel}: {e}")
                await asyncio.sleep(2)  # небольшая задержка между запросами

            # Сохраняем все найденные посты в файл
            if all_posts:
                batch_record = {
                    'timestamp': datetime.now().isoformat(),
                    'posts': all_posts
                }
                try:
                    await storage.save_posts(batch_record)
                    print(f"💾 Сохранено в storage: {len(all_posts)} постов")
                except Exception as e:
                    print(f"❌ Ошибка при сохранении в storage: {e}")

                # Обновляем last_dates.json по каждому каналу
                try:
                    last_dates_dict = await storage.load_all_last_dates()
                    for channel in CONFIG['channels']:
                        channel_posts = [p for p in all_posts if p['channel'] == channel]
                        if channel_posts:
                            # Ищем максимальную дату среди новых постов
                            max_date = max(p['date'] for p in channel_posts)
                            last_dates_dict[channel] = max_date
                    await storage.save_all_last_dates(last_dates_dict)
                except Exception as e:
                    print(f"❌ Ошибка при обновлении last_dates.json: {e}")

                # Публикуем каждый пост с попытками
                for post in all_posts:
                    attempts = 3
                    while attempts > 0:
                        success = await publisher.publish(post)
                        if success:
                            break
                        attempts -= 1
                        print(f"   Попробуем отправить снова. Осталось попыток: {attempts}")
                        await asyncio.sleep(10)

            else:
                print("ℹ️ Нет новых постов для сохранения/публикации")

            # Ждём перед следующим циклом
            await asyncio.sleep(CONFIG['parse_interval'])

    except KeyboardInterrupt:
        print("\n🛑 Безопасное завершение работы бота...")
    finally:
        await tg_client.disconnect()
        print("🔌 Telegram-клиент отключён.")


if __name__ == '__main__':
    asyncio.run(main())
