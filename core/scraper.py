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

        # Сортируем сообщения по ID (от старых к новым)
        messages.sort(key=lambda msg: msg.id)

        # Группируем сообщения по grouped_id
        grouped_messages = {}
        single_messages = []

        for msg in messages:
            if msg.grouped_id:
                if msg.grouped_id not in grouped_messages:
                    grouped_messages[msg.grouped_id] = []
                grouped_messages[msg.grouped_id].append(msg)
            else:
                single_messages.append(msg)

        added = 0
        # Обрабатываем группы (альбомы) ПЕРВЫМИ
        for group_id, group in grouped_messages.items():
            # Сортируем группу по дате (от старых к новым)
            group.sort(key=lambda msg: msg.date)

            # Используем ID первого сообщения в группе как ID поста
            group_id = group[0].id
            print(f"Обрабатываем альбом {group_id} с {len(group)} медиа")
            added += await self._process_group(channel_name, group)

        # Затем обрабатываем одиночные сообщения
        for msg in single_messages:
            # Пропускаем сообщения, которые уже были в группах
            if any(msg in group for group in grouped_messages.values()):
                continue
            print(f"Обрабатываем одиночное сообщение {msg.id}")
            added += await self._process_single_message(channel_name, msg)

        print(f'Занесли в БД {added} новых постов с канала @{channel_name}')
        return added

    async def _process_group(self, channel_name: str, group: list):
        """Обработка группы сообщений (альбом)"""
        # Используем ID первого сообщения в группе
        group_id = group[0].id
        post_dir = Path(self.download_root) / channel_name / str(group_id)
        post_dir.mkdir(parents=True, exist_ok=True)

        post_data = {
            'id': group_id,
            'channel': channel_name,
            'date': group[0].date,  # дата первого сообщения
            'text': "",
            'media': []
        }

        # Объединяем текст (берем текст из последнего сообщения в альбоме)
        last_text = group[-1].raw_text.strip()
        post_data['text'] = last_text if last_text else ""

        # Скачиваем медиа из всех сообщений группы
        for i, msg in enumerate(group):
            if msg.media:
                print(f"Скачиваем медиа {i + 1}/{len(group)} для альбома {group_id}")
                await self._download_media(msg, post_dir, post_data['media'])

        return 1 if await self.db.add_post(post_data) else 0

    async def _download_media(self, message, directory: Path, media_list: list):
        """Скачивает медиа и добавляет данные в media_list"""
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

    async def _process_single_message(self, channel_name: str, msg):
        """Обработка одиночного сообщения"""
        post_dir = Path(self.download_root) / channel_name / str(msg.id)
        post_dir.mkdir(parents=True, exist_ok=True)

        post_data = {
            'id': msg.id,
            'channel': channel_name,
            'date': msg.date,
            'text': msg.raw_text,
            'media': []
        }

        if msg.media:
            await self._download_media(msg, post_dir, post_data['media'])

        return 1 if await self.db.add_post(post_data) else 0

    async def _download_media(self, message, directory: Path, media_list: list):
        """Скачивает медиа и добавляет данные в media_list"""
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
