import os
import asyncio
from pathlib import Path

from telethon import types
from telethon.errors import ChatAdminRequiredError, FloodWaitError


class ChannelPublisher:
    def __init__(self, client, target_channel, media_base_path):
        """
        :param client: Telethon-клиент
        :param target_channel: username или ID целевого канала, куда публикуем
        :param media_base_path: корень папки, где лежат медиа (нужен для рекурсивной очистки)
        """
        self.client = client
        self.target_channel = target_channel
        self.channel_entity = None

        self.max_caption = int(os.getenv('MAX_CAPTION_LENGTH', 1000))
        self.max_media = int(os.getenv('MAX_MEDIA_PER_POST', 10))
        self.delay = int(os.getenv('PUBLISH_DELAY', 5))
        self.media_base = Path(media_base_path)

    async def initialize(self):
        """
        Получаем entity целевого канала (может быть username или ID)
        """
        try:
            self.channel_entity = await self.client.get_entity(self.target_channel)
        except Exception as e:
            print(f"❌ Не удалось получить entity целевого канала {self.target_channel}: {e}")
            raise

    def _prepare_caption(self, text: str) -> str:
        """
        Формирует и обрезает подпись:
          📢 **Новый пост**

          <text>

          _Источник: <target_channel>_
        Гарантируем, что "_Источник_" не будет обрезан полностью.
        """
        header = "📢 **Новый пост**\n\n"
        footer = f"\n\n_Источник: {self.target_channel}_"
        full_text = text.strip()

        # Если длина не превышает лимит, просто склеиваем
        if len(header) + len(full_text) + len(footer) <= self.max_caption:
            return header + full_text + footer

        # Иначе обрезаем так, чтобы footer точно поместился
        allowed = self.max_caption - len(header) - len(footer) - 3  # "-3" нужны под "..."
        truncated = full_text[:allowed] + "..."
        return header + truncated + footer

    def _split_text(self, caption: str):
        """
        Разбивает длинный текст (caption) на части по 4096 символов (лимит Telegram).
        Возвращает список частей.
        """
        parts = []
        for i in range(0, len(caption), 4096):
            parts.append(caption[i:i+4096])
        return parts

    async def _send_media_group(self, media_paths, caption):
        """
        Отправляет альбом (несколько фотографий/документов).
        Первый элемент получает caption.
        """
        media_objects = []
        for idx, path in enumerate(media_paths[:self.max_media]):
            if not os.path.exists(path):
                continue
            # Определяем тип
            if path.lower().endswith(('.jpg', '.jpeg', '.png')):
                media_class = types.InputMediaPhoto
            else:
                media_class = types.InputMediaDocument

            # На первый файл в альбоме вешаем caption
            if idx == 0:
                media = media_class(media=path, caption=caption, parse_mode='markdown')
            else:
                media = media_class(media=path)
            media_objects.append(media)

        if media_objects:
            try:
                await self.client.send_file(
                    self.channel_entity,
                    media_objects,
                    supports_streaming=True
                )
            except Exception as e:
                print(f"❌ Ошибка при отправке media_group: {e}")
                raise

    async def _send_single_media(self, path, caption):
        """
        Отправляет одиночное медиа с подписью.
        """
        try:
            await self.client.send_file(
                self.channel_entity,
                path,
                caption=caption,
                parse_mode='markdown',
                supports_streaming=True
            )
        except Exception as e:
            print(f"❌ Ошибка при отправке single_media: {e}")
            raise

    async def _send_text(self, text_parts):
        """
        Если нет медиа, просто отправляем текстовые части по очереди.
        """
        for part in text_parts:
            try:
                await self.client.send_message(
                    self.channel_entity,
                    part,
                    parse_mode='markdown',
                    link_preview=False
                )
            except Exception as e:
                print(f"❌ Ошибка при отправке текста: {e}")
                raise
            await asyncio.sleep(self.delay)

    async def _clean_media(self, media_paths):
        """
        Удаляет после публикации файлы, а затем рекурсивно удаляет пустые папки вплоть до media_base.
        """
        for path in media_paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    dir_path = Path(path).parent
                    # Удаляем пустые каталоги наверх, пока не дойдём до root media_base
                    while dir_path.exists() and dir_path != self.media_base and not any(dir_path.iterdir()):
                        dir_path.rmdir()
                        dir_path = dir_path.parent
            except Exception as e:
                print(f"❌ Ошибка очистки медиа-файла {path}: {e}")

    async def publish(self, post: dict) -> bool:
        """
        Основной метод публикации:
          1) Формируем caption
          2) Проверяем, есть ли медиа (существуют ли файлы)
          3) Если медиа>1: _send_media_group, если =1: _send_single_media
             Если нет медиа: _send_text
          4) После успешной отправки удаляем медиа-файлы
        Возвращает True, если всё прошло успешно, иначе False.
        """
        try:
            caption = self._prepare_caption(post['text'])
            # Собираем список существующих файлов медиа
            media_list = [m['path'] for m in post.get('media', []) if os.path.exists(m['path'])]

            if media_list:
                if len(media_list) > 1:
                    await self._send_media_group(media_list, caption)
                else:
                    await self._send_single_media(media_list[0], caption)
            else:
                # Если нет медиа, отправляем текст
                parts = self._split_text(caption)
                await self._send_text(parts)

            # После успешной отправки - чистим скачанные медиа
            await self._clean_media([m['path'] for m in post.get('media', [])])
            return True

        except ChatAdminRequiredError:
            print("❌ Ошибка: боту требуются права администратора в целевом канале.")
            return False
        except FloodWaitError as e:
            print(f"⚠️ Telegram просит подождать {e.seconds} секунд (FloodWait).")
            await asyncio.sleep(e.seconds)
            return False
        except Exception as e:
            print(f"❌ Неожиданная ошибка при публикации: {e}")
            return False
