import os
import asyncio
from datetime import datetime
from pathlib import Path

from telethon import errors


class TelegramScraper:
    def __init__(self, client, vector_db, storage):
        """
        :param client: Telethon-клиент (TelegramClient)
        :param vector_db: экземпляр VectorDatabase
        :param storage: экземпляр DataManager
        """
        self.client = client
        self.vector_db = vector_db
        self.storage = storage
        self.media_path = os.getenv('MEDIA_SAVE_PATH', 'media')

    async def _download_media(self, message, channel_name):
        """
        Скачивает все медиафайлы из сообщения и возвращает список {'path': ..., 'type': ...}
        Файлы сохраняются в папке: media/<channel_name>/<YYYY-MM-DD>/<message_id>/
        """
        media_items = []
        try:
            if message.media:
                # Формируем путь: media/<channel_name>/<YYYY-MM-DD>/<message_id>/
                date_folder = message.date.strftime('%Y-%m-%d')
                save_dir = Path(self.media_path) / channel_name / date_folder / str(message.id)
                save_dir.mkdir(parents=True, exist_ok=True)

                # Telethon Message может иметь несколько типов media: photo, video, document, audio и т. д.
                for attr in ('photo', 'video', 'document', 'audio'):
                    media_obj = getattr(message, attr, None)
                    if media_obj:
                        # Скачиваем под уникальным именем
                        file_path = await self.client.download_media(
                            media_obj,
                            file=save_dir / f"{attr}_{media_obj.id}"
                        )
                        if file_path:
                            media_items.append({
                                'path': str(file_path),
                                'type': attr
                            })
        except Exception as e:
            print(f"❌ Ошибка при скачивании медиа (msg {message.id}): {e}")
        return media_items

    async def _create_post(self, message, channel_name):
        """
        Создаёт словарь-пост с полями:
        {
          'channel': <имя канала>,
          'date': <ISO-строка даты>,
          'text': <текст или подпись>,
          'id': <message.id>,
          'media': [ {'path':..., 'type':...}, ... ]
        }
        """
        # Получаем текст: либо обычный text, либо подпись к медиа (message.message / message.caption)
        text = message.text or getattr(message, 'message', None) or getattr(message, 'caption', None) or ""
        media = await self._download_media(message, channel_name)
        return {
            'channel': channel_name,
            'date': message.date.isoformat(),
            'text': text.strip(),
            'id': message.id,
            'media': [
                {'path': m['path'], 'type': m['type']} for m in media
            ]
        }

    async def scrape_channel(self, channel_username, limit=30):
        """
        Парсит канал `channel_username`, возвращает список новых постов (прохождение до последнего saved_date).
        Логика:
          1. Читаем последнюю дату, до которой мы уже парсили (из last_dates. json).
          2. Итерируем сообщения начиная с самых свежих; пока message.date > last_date — проверяем дубликаты и формируем новые посты.
        """
        try:
            # Получаем entity канала
            channel = await self.client.get_entity(channel_username)
        except errors.UsernameInvalidError:
            print(f"❌ Некорректный username или канал не найден: {channel_username}")
            return []
        except Exception as e:
            print(f"❌ Ошибка get_entity для {channel_username}: {e}")
            return []

        # Считываем last_date (ISO-строка) для этого канала из storage
        last_date_iso = await self.storage.get_last_date(channel_username)
        if last_date_iso:
            try:
                last_date = datetime.fromisoformat(last_date_iso)
            except Exception:
                last_date = None
        else:
            last_date = None

        new_posts = []
        counter = 0

        async for message in self.client.iter_messages(channel, limit=limit):
            # Если прошли до сообщений старше или равных последней сохранённой даты, прекращаем
            if last_date and message.date <= last_date:
                break

            # Берём текст для эмбеддинга
            text = message.text or getattr(message, 'message', None) or getattr(message, 'caption', None) or ""
            text = text.strip()

            # Генерируем нормированный эмбеддинг и проверяем, не дубликат ли он
            embedding = self.vector_db.text_to_embedding(text)
            if self.vector_db.is_duplicate(embedding):
                # Такой текст/подпись уже был — пропускаем, не скачиваем медиа
                continue

            # Если не дубликат, создаём полный пост (со скачиванием медиа)
            post = await self._create_post(message, channel_username)
            # Добавляем пост в векторную БД (сохраняем эмбеддинг внутри)
            added = self.vector_db.add_post(post)
            if not added:
                # На всякий случай: векторная БД могла сказать, что это дубликат (например, из-за погрешности),
                # тогда удаляем скачанные файлы, если они были
                for m in post.get('media', []):
                    try:
                        if os.path.exists(m['path']):
                            os.remove(m['path'])
                            dir_path = Path(m['path']).parent
                            if not any(dir_path.iterdir()):
                                dir_path.rmdir()
                    except Exception:
                        pass
                continue

            new_posts.append(post)
            counter += 1

        print(f"   Проверено сообщений: {counter}. Новых добавлено: {len(new_posts)}.")
        return new_posts
