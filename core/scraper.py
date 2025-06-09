from pathlib import Path

class TGScraper:
    def __init__(self, client, post_limit: int, db, download_root: str = "media"):
        self.client = client
        self.post_limit = post_limit
        self.db = db
        self.download_root = download_root

    async def scrape_posts_from_one_channel(self, channel_name: str):
        channel = await self.client.get_entity(channel_name)
        all_messages = []
        offset_id = 0
        posts_processed = 0

        while posts_processed < self.post_limit:
            batch_size = max(20, (self.post_limit - posts_processed) * 7)
            messages = await self.client.get_messages(
                channel,
                limit=batch_size,
                offset_id=offset_id
            )

            if not messages:
                break

            messages.sort(key=lambda msg: msg.id, reverse=True)

            for msg in messages:
                if not (msg.text or msg.media):
                    continue

                if not hasattr(msg, 'grouped_id') or not msg.grouped_id:
                    posts_processed += 1

                all_messages.append(msg)

                if posts_processed >= self.post_limit:
                    break

            if messages:
                offset_id = messages[-1].id - 1

        print(f'Получили {len(all_messages)} сообщений для {self.post_limit} постов с канала @{channel_name}')

        added = await self.process_messages(channel_name, all_messages)
        print(f'Занесли в БД {added} новых постов с канала @{channel_name}')
        return added

    async def process_messages(self, channel_name: str, messages: list):
        messages.sort(key=lambda msg: msg.id)

        grouped_messages = {}
        single_messages = []
        processed_groups = set()

        for msg in messages:
            if not (msg.text or msg.media):
                continue

            if msg.grouped_id:
                if msg.grouped_id in processed_groups:
                    continue

                if msg.grouped_id not in grouped_messages:
                    grouped_messages[msg.grouped_id] = []
                grouped_messages[msg.grouped_id].append(msg)
            else:
                single_messages.append(msg)

        added = 0

        for group_id, group in grouped_messages.items():
            processed_groups.add(group_id)

            group.sort(key=lambda msg: msg.date)

            group_id_val = group[0].id
            print(f"Обрабатываем альбом {group_id_val} с {len(group)} медиа")
            added += await self._process_group(channel_name, group)

        for msg in single_messages:
            if any(msg in group for group in grouped_messages.values()):
                continue
            print(f"Обрабатываем одиночное сообщение {msg.id}")
            added += await self._process_single_message(channel_name, msg)

        return added

    async def _process_group(self, channel_name: str, group: list):
        group_id = group[0].id
        post_dir = Path(self.download_root) / channel_name / str(group_id)
        post_dir.mkdir(parents=True, exist_ok=True)

        post_data = {
            'id': group_id,
            'channel': channel_name,
            'date': group[0].date,
            'text': "",
            'media': []
        }

        texts = []
        for msg in group:
            if msg.text and msg.text.strip():
                texts.append(msg.text.strip())

        post_data['text'] = "\n\n".join(texts) if texts else ""

        for i, msg in enumerate(group):
            if msg.media:
                print(f"Скачиваем медиа {i + 1}/{len(group)} для альбома {group_id}")
                await self._download_media(msg, post_dir, post_data['media'])

        return 1 if await self.db.add_post(post_data) else 0

    async def _process_single_message(self, channel_name: str, msg):
        post_dir = Path(self.download_root) / channel_name / str(msg.id)
        post_dir.mkdir(parents=True, exist_ok=True)

        post_data = {
            'id': msg.id,
            'channel': channel_name,
            'date': msg.date,
            'text': msg.text,
            'media': []
        }

        if msg.media:
            await self._download_media(msg, post_dir, post_data['media'])

        return 1 if await self.db.add_post(post_data) else 0

    async def _download_media(self, message, directory: Path, media_list: list):
        try:
            downloaded = await self.client.download_media(message, file=directory)
            paths = downloaded if isinstance(downloaded, list) else [downloaded]

            for path in paths:
                if message.photo:
                    mtype = "photo"
                elif message.document:
                    if message.document.mime_type.startswith('video'):
                        mtype = "video"
                    else:
                        mtype = (message.document.mime_type or "document").split("/")[-1]
                else:
                    mtype = "unknown"

                media_list.append({
                    'type': mtype,
                    'file_path': str(path),
                })
        except Exception as e:
            print(f"Ошибка при скачивании медиа: {e}")