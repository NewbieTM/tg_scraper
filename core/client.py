from telethon import TelegramClient
from os.path import exists

class TelegramClientManager:
    def __init__(self, session_file, api_id, api_hash, phone):
        self.client = TelegramClient(session_file, api_id, api_hash)
        self.phone = phone

    async def start(self):
        """Инициализирует клиент Telegram"""
        if not exists(f'{self.client.session.filename}.session'):
            await self.client.start(self.phone)
        else:
            await self.client.start()
        return self.client