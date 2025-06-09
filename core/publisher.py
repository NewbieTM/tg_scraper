from telethon import TelegramClient
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from core.db_manager import DBManager
from core.db_models import Post, Media
from pathlib import Path
import asyncio
import os


class PostPublisher:
    def __init__(self, client: TelegramClient, db_manager: DBManager, target_channel: str, post_delay):
        self.client = client
        self.db_manager = db_manager
        self.target_channel = target_channel
        self.post_delay = post_delay

    async def publish_posts(self):
        posts = await self._get_unpublished_posts()
        for post in reversed(posts):
            try:
                print(f"[PUBLISH] Публикую пост {post.post_id} из канала @{post.channel_name}")
                success = await self._publish_post(post)

                if success:
                    # Помечаем пост как опубликованный
                    marked = await self.db_manager.mark_post_published(post.post_id, post.channel_name)

                    # Удаляем медиа после успешной публикации
                    if marked:
                        await self._cleanup_media(post)
                        print(f"[SUCCESS] Пост {post.post_id} опубликован, медиа удалены")
                else:
                    print(f"[WARNING] Пост {post.post_id} не опубликован")

                await asyncio.sleep(self.post_delay)
            except Exception as e:
                print(f"[ERROR] Не удалось опубликовать пост {post.post_id}: {e}")

    async def _get_unpublished_posts(self) -> list[Post]:
        async with self.db_manager.async_session() as session:
            result = await session.execute(
                select(Post)
                .options(selectinload(Post.media))
                .where(Post.published == False)
                .order_by(Post.date.desc())
            )
            return result.scalars().all()

    async def _publish_post(self, post: Post) -> bool:
        # Фильтруем только существующие медиа-файлы
        media_files = []
        for media in post.media:
            media_path = Path(media.file_path)
            if media_path.exists():
                media_files.append(media_path)
            else:
                print(f"[WARNING] Медиафайл не найден: {media_path}")

        try:
            if not media_files and not post.text.strip():
                print(f"[SKIPPED] Пост {post.post_id} не содержит контента")
                return False

            if not media_files:
                await self.client.send_message(
                    self.target_channel,
                    post.text,
                    parse_mode='md'
                )
                return True
            else:
                await self.client.send_file(
                    self.target_channel,
                    [str(f) for f in media_files],
                    caption=post.text if post.text.strip() else None,
                    parse_mode='md',
                    force_document=False
                )
                return True
        except Exception as e:
            print(f"[ERROR] Ошибка публикации: {e}")
            return False

    async def _cleanup_media(self, post: Post):
        """Удаляет медиа-файлы после успешной публикации"""
        for media in post.media:
            try:
                media_path = Path(media.file_path)
                if media_path.exists():
                    media_path.unlink()
                    print(f"Удален медиафайл: {media_path}")

                # Удаляем пустые директории
                media_dir = media_path.parent
                if media_dir.exists() and not any(media_dir.iterdir()):
                    media_dir.rmdir()
                    print(f"Удалена пустая директория: {media_dir}")
            except Exception as e:
                print(f"Ошибка при удалении медиа: {e}")