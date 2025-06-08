import os
import asyncio
from pathlib import Path
from telethon import types
from telethon.errors import FloodWaitError, ChatAdminRequiredError, UserPrivacyRestrictedError


class ChannelPublisher:

    def __init__(self, client, target_channel: str, delay: int = 2):
        self.client = client
        self.target_channel = target_channel
        self.channel_entity = None
        self.delay = delay

    async def initialize(self):
        try:
            self.channel_entity = await self.client.get_entity(self.target_channel)
        except Exception as e:
            raise RuntimeError(f"Ошибка получения канала {self.target_channel}: {e}")

    def _prepare_caption(self, post: dict) -> str:
        base_text = post.get('text', '') or ''
        source = f"\n\nИсточник: @{post['channel']}" if post.get('channel') else ''
        full_caption = base_text + source
        return full_caption[:self.max_caption]

    async def _send_text(self, caption: str) -> bool:
        if not caption:
            return False

        parts = [caption[i:i + 4096] for i in range(0, len(caption), 4096)]
        try:
            for part in parts:
                await self.client.send_message(
                    self.channel_entity,
                    part,
                    link_preview=False
                )
                await asyncio.sleep(self.delay)
            return True
        except Exception as e:
            print(f"  ❌ Ошибка отправки текста: {e}")
            return False

    async def _send_media_album(self, media_list: list, caption: str) -> bool:
        """
        Отправляет альбом медиа с подписью.
        """
        valid_media = []
        for media in media_list:
            path = Path(media['path'])
            if path.exists():
                valid_media.append(path)

        if not valid_media:
            return False

        try:
            # Первый элемент с подписью, остальные без
            album_entities = [valid_media[0]]
            if caption:
                album_entities[0] = (album_entities[0], caption)

            if len(valid_media) > 1:
                album_entities.extend(valid_media[1:])

            await self.client.send_file(
                self.channel_entity,
                file=album_entities,
                supports_streaming=True
            )
            return True
        except FloodWaitError as e:
            print(f"  ⏱️ FloodWait: ждем {e.seconds} сек.")
            await asyncio.sleep(e.seconds)
            return False
        except Exception as e:
            print(f"  ❌ Ошибка отправки альбома: {e}")
            return False

    async def publish(self, post: dict) -> bool:
        caption = self._prepare_caption(post)
        media_list = post.get('media', [])
        is_album = post.get('is_album', False)

        try:
            # Отправка альбома
            if is_album and media_list:
                return await self._send_media_album(media_list, caption)

            # Отправка одиночного медиа
            elif media_list:
                media = media_list[0]
                path = Path(media['path'])
                if not path.exists():
                    print(f"  ⚠️ Файл {path} не найден")
                    return False

                await self.client.send_file(
                    self.channel_entity,
                    file=path,
                    caption=caption,
                    supports_streaming=True
                )
                return True

            # Отправка текста
            else:
                return await self._send_text(caption)

        except Exception as e:
            print(f"  ❌ Ошибка публикации: {e}")
            return False

    async def clean_media(self, post: dict) -> None:
        for media in post.get('media', []):
            try:
                path = Path(media['path'])
                if path.exists():
                    path.unlink()
            except Exception as e:
                print(f"  ⚠️ Ошибка удаления {path}: {e}")