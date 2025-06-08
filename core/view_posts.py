import asyncio
from core.db_manager import DBManager
from dotenv import load_dotenv
from sqlalchemy import text


load_dotenv()

async def show_posts():
    db = DBManager()
    await db.initialize()

    async with db.async_session() as session:
        result = await session.execute(text("SELECT * FROM posts ORDER BY date DESC LIMIT 10;"))
        posts = result.fetchall()
        for post in posts:
            print(post)

    await db.close()

if __name__ == "__main__":
    asyncio.run(show_posts())