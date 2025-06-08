from pathlib import Path

class TGScraper:
    def __init__(self, client, post_limit: int, db, download_root: str = "media"):
        self.client = client
        self.post_limit = post_limit
        self.db = db
        self.download_root = download_root

    async def scrape_posts_from_one_channel(self, channel_name: str):
        channel = await self.client.get_entity(channel_name)
        messages = await self.client.get_messages(channel, limit=self.post_limit)
        print(f'Получили {len(messages)} постов с канала @{channel_name}')

        added = 0
        for msg in (messages):
            post_dir = Path(self.download_root) / channel_name / str(msg.id)
            post_dir.mkdir(parents=True, exist_ok=True)

            post_data = {
                'id': msg.id,
                'channel': channel_name,
                'date': msg.date,
                'text': msg.raw_text, # .raw_text, .text, .message
                'media': []
            }

            if msg.media:
                downloaded = await self.client.download_media(
                    msg, file=post_dir
                )
                if isinstance(downloaded, list):
                    paths = downloaded
                else:
                    paths = [downloaded]

                print(f"Скачаны файлы для сообщения {msg.id}: {len(paths)}")

                for path in paths:
                    if msg.photo:
                        mtype = "photo"
                    elif msg.document:
                        if msg.document.mime_type.startswith('video'):
                            mtype = "video"
                        else:
                            mtype = (msg.document.mime_type or "document").split("/")[-1]
                    else:
                        mtype = "unknown"

                    post_data['media'].append({
                        'type': mtype,
                        'file_path': str(path),
                    })

            ok = await self.db.add_post(post_data)
            if ok:
                added += 1

        print(f'Занесли в БД {added} новых постов с канала @{channel_name}')
        return added
