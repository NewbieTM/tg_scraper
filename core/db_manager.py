import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .db_models import Base, Post, Media
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError


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


    async def add_post(self, post_data: dict) -> bool:
        async with self.async_session() as session:
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



    async def close(self):
        await self.engine.dispose()