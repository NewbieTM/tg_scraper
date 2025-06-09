import os
from dotenv import load_dotenv
import asyncio
import sys
import shutil

from core.client import TelegramClientManager
from core.scraper import TGScraper
from core.db_manager import DBManager
from core.publisher import PostPublisher

load_dotenv()

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
    'API_ID': os.environ.get('API_ID'),
    'API_HASH': os.environ.get('API_HASH'),
    'PHONE': os.environ.get('PHONE'),
    'SESSION_FILE': os.environ.get('SESSION_FILE', 'tg_session'),
    'CHANNELS': [x.strip() for x in os.getenv('CHANNELS').split(',') if x.strip()],
    'MY_CHANNEL': os.environ.get('MY_CHANNEL'),
    'POST_LIMIT': int(os.environ.get('POST_LIMIT', 5)),
    'PARSE_INTERVAL': int(os.environ.get('PARSE_INTERVAL', 3600)),
    'PUBLISH_DELAY': int(os.environ.get('PUBLISH_DELAY', 10)),
}

async def main():

    media_folder = 'media'
    if os.path.exists(media_folder):
        shutil.rmtree(media_folder)
    os.makedirs(media_folder, exist_ok=True)


    tg_client_manager = TelegramClientManager(CONFIG['SESSION_FILE'], CONFIG['API_ID'], CONFIG['API_HASH'], CONFIG['PHONE'])
    try:
         tg_client = await tg_client_manager.start()
    except Exception as e:
        print(f"❌ Не удалось запустить TelegramClientManager: {e}")
        return

    db = DBManager()
    try:
        await db.initialize()
    except Exception as e:
        print(f"❌ Ошибка инициализации БД: {e}")
        await tg_client.disconnect()
        return


    scraper = TGScraper(tg_client, CONFIG['POST_LIMIT'], db, 'media')

    publisher = PostPublisher(tg_client, db, target_channel=CONFIG['MY_CHANNEL'], post_delay=CONFIG['PUBLISH_DELAY'])

    while True:
        for channel in CONFIG['CHANNELS']:
            await scraper.scrape_posts_from_one_channel(channel)
            print(f'Посты с канала {channel} распаршены, перехожу к следующему каналу')

        await publisher.publish_posts()
        print(f'Все посты опубликованы, следующий парсинг через {CONFIG['PARSE_INTERVAL']}')
        await asyncio.sleep(CONFIG['PARSE_INTERVAL'])



if __name__ == '__main__':
    asyncio.run(main())