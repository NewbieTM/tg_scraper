import os
from pathlib import Path
from telethon import types
from telethon.errors import RPCError
from .db_manager import DatabaseManager


class TelegramScraper:
    def __init__(self, client, db_manager: DatabaseManager, media_path: str):
        self.client = client
        self.db_manager = db_manager
        self.media_path = media_path

    async def _download_media(self, message: types.Message, channel_name: str) -> list:
        """
        Скачивает все медиафайлы из сообщения.
        """
        media_paths = []
        if not message.media:
            return media_paths

        save_dir = Path(self.media_path) / channel_name / str(message.id)
        save_dir.mkdir(parents=True, exist_ok=True)

        try:
            if isinstance(message.media, types.MessageMediaPhoto):
                media_type = 'photo'
                file_path = await self.client.download_media(
                    message.media,
                    file=save_dir / f'photo_{message.id}.jpg'
                )
                if file_path:
                    media_paths.append({'path': str(file_path), 'type': media_type})

            elif isinstance(message.media, types.MessageMediaDocument):
                doc = message.media.document
                mime = doc.mime_type or ''
                if mime.startswith('video/'):
                    media_type = 'video'
                    ext = '.mp4'
                elif mime.startswith('audio/'):
                    media_type = 'audio'
                    ext = '.mp3'
                elif mime.startswith('image/'):
                    media_type = 'photo'
                    ext = '.jpg'
                else:
                    media_type = 'document'
                    ext = '.bin'

                file_path = await self.client.download_media(
                    message.media,
                    file=save_dir / f'doc_{message.id}{ext}'
                )
                if file_path:
                    media_paths.append({'path': str(file_path), 'type': media_type})
        except Exception as e:
            print(f"  ❌ Ошибка загрузки медиа (msg_id={message.id}): {e}")

        return media_paths

    async def _create_post(self, message: types.Message, channel_name: str) -> dict:
        """
        Формирует словарь с данными поста.
        """
        media = await self._download_media(message, channel_name)
        text = message.message or message.text or ''

        posted_at = message.date
        if posted_at and posted_at.tzinfo is not None:
            posted_at = posted_at.replace(tzinfo=None)

        return {
            'channel': channel_name,
            'id': message.id,
            'date': posted_at,
            'text': text,
            'media': media
        }

    async def scrape_channel(self, channel_username: str, limit: int = 8) -> list:
        """
        Парсит канал с группировкой альбомов.
        """
        new_posts = []
        try:
            entity = await self.client.get_entity(channel_username)
        except RPCError as e:
            print(f"  ❌ Ошибка получения entity для {channel_username}: {e}")
            return new_posts

        current_group = []
        try:
            async for message in self.client.iter_messages(entity, limit=limit):
                # Пропускаем служебные сообщения
                if not message.message and not message.media:
                    continue

                # Обработка группированных сообщений (альбомы)
                if message.grouped_id:
                    if current_group and current_group[0].grouped_id != message.grouped_id:
                        # Новая группа, обрабатываем предыдущую
                        group_post = await self._process_group(current_group, channel_username)
                        if group_post:
                            new_posts.append(group_post)
                        current_group = [message]
                    else:
                        current_group.append(message)
                else:
                    if current_group:
                        # Завершаем текущую группу
                        group_post = await self._process_group(current_group, channel_username)
                        if group_post:
                            new_posts.append(group_post)
                        current_group = []

                    # Обработка одиночного сообщения
                    post_data = await self._create_post(message, channel_username)
                    added = await self.db_manager.add_post(post_data)
                    if added:
                        new_posts.append(post_data)

            # Обработка оставшейся группы
            if current_group:
                group_post = await self._process_group(current_group, channel_username)
                if group_post:
                    new_posts.append(group_post)

        except Exception as e:
            print(f"  ❌ Ошибка итерации по {channel_username}: {e}")

        return new_posts

    async def _process_group(self, messages: list, channel_name: str) -> dict:
        """
        Обрабатывает группу сообщений как один пост.
        """
        if not messages:
            return None

        # Скачиваем медиа из всех сообщений группы
        all_media = []
        for msg in messages:
            media = await self._download_media(msg, channel_name)
            all_media.extend(media)

        # Берем данные из первого сообщения
        first_msg = messages[0]
        text = first_msg.message or first_msg.text or ''

        posted_at = first_msg.date
        if posted_at and posted_at.tzinfo is not None:
            posted_at = posted_at.replace(tzinfo=None)

        post_data = {
            'channel': channel_name,
            'id': first_msg.id,
            'date': posted_at,
            'text': text,
            'media': all_media,
            'is_album': True  # Флаг альбома
        }

        # Добавляем в БД
        added = await self.db_manager.add_post(post_data)
        return post_data if added else None