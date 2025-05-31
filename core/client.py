from telethon import TelegramClient
from os.path import exists


class TelegramClientManager:
    def __init__(self, session_file, api_id, api_hash, phone):
        # В session_file ожидаем имя без расширения ".session"
        self.session_name = session_file
        self.client = TelegramClient(self.session_name, api_id, api_hash)
        self.phone = phone

    async def start(self):
        """
        Запускает TelegramClient. Проверяем, существует ли файл "<session_name>.session".
        Если нет — вызываем client.start(self.phone), чтобы произвести логин через код.
        Иначе — client.start() запустит клиента по сессии.
        """
        session_path = f"{self.session_name}.session"
        if not exists(session_path):
            await self.client.start(self.phone)
        else:
            await self.client.start()
        return self.client
