import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .db_models import Base, Post, Media
from sqlalchemy import update, delete, and_, func, select
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta, timezone


class DBManager:
    def __init__(self):
        db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.db_url = (
            f"postgresql+asyncpg://{db_config['user']}:"
            f"{db_config['password']}@{db_config['host']}:"
            f"{db_config['port']}/{db_config['database']}"
        )
        self.engine = create_async_engine(self.db_url, echo=False)
        self.async_session = sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    async def initialize(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        print("✅ База данных инициализирована")
        await self.cleanup_old_posts(days=3)

    async def post_exists(self, post_id: int, channel_name: str) -> bool:
        """Проверяет, существует ли пост в базе"""
        async with self.async_session() as session:
            result = await session.execute(
                select(Post).where(
                    Post.post_id == post_id,
                    Post.channel_name == channel_name
                )
            )
            return result.scalar() is not None

    async def add_post(self, post_data: dict) -> bool:
        async with self.async_session() as session:
            # Проверяем, существует ли пост
            exists = await self.post_exists(post_data['id'], post_data['channel'])
            if exists:
                return False

            new_post = Post(
                post_id=post_data['id'],
                channel_name=post_data['channel'],
                date=post_data['date'],
                text=post_data['text']
            )

            for m in post_data.get('media', []):
                new_post.media.append(
                    Media(
                        post_id=post_data['id'],
                        channel_name=post_data['channel'],
                        media_type=m['type'],
                        file_path=m['file_path']
                    )
                )

            session.add(new_post)
            try:
                await session.commit()
                return True
            except IntegrityError:
                await session.rollback()
                return False
            except Exception as e:
                await session.rollback()
                print(f"❌ Ошибка добавления поста: {e}")
                return False

    async def mark_post_published(self, post_id: int, channel: str) -> bool:
        async with self.async_session() as session:
            try:
                stmt = (
                    update(Post)
                    .where(
                        Post.post_id == post_id,
                        Post.channel_name == channel
                    )
                    .values(published=True)
                    .execution_options(synchronize_session="fetch")
                )
                result = await session.execute(stmt)
                await session.commit()
                return result.rowcount > 0
            except Exception as e:
                await session.rollback()
                print(f"❌ Ошибка отметки публикации: {e}")
                return False

    async def cleanup_old_posts(self, days: int = 3):
        """Удаляет неопубликованные посты старше указанного количества дней"""
        async with self.async_session() as session:
            try:
                threshold = datetime.now(timezone.utc) - timedelta(days=days)

                # Сначала получаем медиа для удаления
                media_to_delete = await session.execute(
                    select(Media.file_path)
                    .join(Post)
                    .where(
                        and_(
                            Post.published == False,
                            Post.date < threshold
                        )
                    )
                )
                media_files = [m[0] for m in media_to_delete.all()]

                # Удаляем посты из БД
                stmt = delete(Post).where(
                    and_(
                        Post.published == False,
                        Post.date < threshold
                    )
                )
                result = await session.execute(stmt)
                await session.commit()

                # Удаляем файлы медиа
                for file_path in media_files:
                    try:
                        path = Path(file_path)
                        if path.exists():
                            path.unlink()
                    except Exception as e:
                        print(f"Ошибка удаления файла {file_path}: {e}")

                print(f"🗑️ Удалено {result.rowcount} старых неопубликованных постов")
                return result.rowcount
            except Exception as e:
                await session.rollback()
                print(f"❌ Ошибка очистки старых постов: {e}")
                return 0

    async def close(self):
        await self.engine.dispose()