from telethon import TelegramClient
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from core.db_manager import DBManager
from core.db_models import Post
from pathlib import Path
import asyncio

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
                await self._publish_post(post)
                await self.db_manager.mark_post_published(post.post_id, post.channel_name)
                print(f"[SUCCESS] Пост {post.post_id} успешно опубликован")
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

    async def _publish_post(self, post: Post):
        media_files = [Path(media.file_path) for media in post.media]

        for media_file in media_files:
            if not media_file.exists():
                raise FileNotFoundError(f"Медиафайл не найден: {media_file}")
        if not media_files:
            await self.client.send_message(self.target_channel, post.text, parse_mode='md')
        else:
            await self.client.send_file(
                self.target_channel,
                [str(f) for f in media_files],
                caption=post.text,
                parse_mode='md',
                force_document=False
            )