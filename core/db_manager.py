import os
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .db_models import Base, Post, Media

class DatabaseManager:
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

    async def add_post(self, post_data: dict) -> bool:
        async with self.async_session() as session:
            try:
                # Проверка существования поста
                stmt = select(Post).where(
                    Post.post_id == post_data['id'],
                    Post.channel == post_data['channel']
                )
                result = await session.execute(stmt)
                if result.scalars().first():
                    return False

                new_post = Post(
                    post_id=post_data['id'],
                    channel=post_data['channel'],
                    post_date=post_data['date'],
                    post_text=post_data['text'],
                    is_album=post_data.get('is_album', False)
                )

                for m in post_data.get('media', []):
                    new_media = Media(
                        file_path=m['path'],
                        media_type=m['type']
                    )
                    new_post.media.append(new_media)

                session.add(new_post)
                await session.commit()
                return True
            except Exception as e:
                await session.rollback()
                print(f"  ❌ Ошибка добавления поста: {e}")
                return False

    async def mark_post_published(self, post_id: int, channel: str) -> bool:
        async with self.async_session() as session:
            try:
                stmt = select(Post).where(
                    Post.post_id == post_id,
                    Post.channel == channel
                )
                result = await session.execute(stmt)
                post = result.scalars().first()
                if post:
                    post.published = True
                    await session.commit()
                    return True
                return False
            except Exception as e:
                await session.rollback()
                print(f"  ❌ Ошибка отметки публикации: {e}")
                return False

    async def close(self):
        await self.engine.dispose()