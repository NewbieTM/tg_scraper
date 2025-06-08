from telethon import TelegramClient
from os.path import exists

class TelegramClientManager:
    def __init__(self, session_file, api_id, api_hash, phone):
        self.client = TelegramClient(session_file, api_id, api_hash)
        self.phone = phone
        self.session_file = session_file

    async def start(self):
        if not exists(f'{self.session_file}.session'):
            await self.client.start(self.phone)
            self.client.session.save()
        else:
            await self.client.start()
        return self.client