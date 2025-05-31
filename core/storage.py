import aiofiles
import json
from datetime import datetime, timedelta
from pathlib import Path


class DataManager:
    def __init__(self, output_file='posts.json', last_dates_file='last_dates.json'):
        """
        :param output_file: путь до файла, в который мы будем дозаписывать JSON-строки с пачками постов
        :param last_dates_file: путь до файла last_dates.json, где храним { "channel_name": "ISO-date", ... }
        """
        self.output_file = Path(output_file)
        self.last_dates_file = Path(last_dates_file)
        # Создаём сам файл last_dates.json, если его нет
        self.last_dates_file.parent.mkdir(parents=True, exist_ok=True)
        self.last_dates_file.touch(exist_ok=True)

    async def save_posts(self, batch_dict):
        """
        Дозаписывает в output_file одну строку с JSON-представлением batch_dict:
        { "timestamp": <ISO>, "posts": [ ... ] }
        """
        if not batch_dict.get('posts'):
            return

        async with aiofiles.open(self.output_file, 'a', encoding='utf-8') as f:
            # Если файла не было, то он создался. В дальнейшем просто дозапись.
            json_str = json.dumps(batch_dict, ensure_ascii=False, indent=2)
            await f.write(json_str + '\n')

    async def clean_old_data(self, retention_days):
        """
        Очищает устаревшие батчи из output_file, сравнивая 'timestamp' в каждой строке.
        """
        cutoff = datetime.now() - timedelta(days=retention_days)
        if not self.output_file.exists():
            return

        # Читаем весь файл
        async with aiofiles.open(self.output_file, 'r', encoding='utf-8') as f:
            content = await f.read()

        lines = [line for line in content.split('\n') if line.strip()]
        new_lines = []
        for line in lines:
            try:
                obj = json.loads(line)
                ts = datetime.fromisoformat(obj.get('timestamp'))
                if ts > cutoff:
                    new_lines.append(line)
            except Exception:
                # Если строчка не валидный JSON или без timestamp — оставляем, чтобы не сломать всё
                new_lines.append(line)

        # Перезаписываем файл
        async with aiofiles.open(self.output_file, 'w', encoding='utf-8') as f:
            for ln in new_lines:
                await f.write(ln + '\n')

    async def load_all_last_dates(self):
        """
        Возвращает весь словарь last_dates = { "channel1": "2025-05-31T12:00:00", ... }
        Если файл пуст, возвращает {}.
        """
        try:
            async with aiofiles.open(self.last_dates_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                if not content.strip():
                    return {}
                return json.loads(content)
        except Exception:
            return {}

    async def save_all_last_dates(self, last_dates_dict):
        """
        Перезаписывает last_dates_file содержимым словаря last_dates_dict.
        """
        async with aiofiles.open(self.last_dates_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(last_dates_dict, indent=2, ensure_ascii=False))

    async def get_last_date(self, channel):
        """
        Возвращает ISO-строку даты последнего поста для данного канала (или None).
        """
        last_dates = await self.load_all_last_dates()
        return last_dates.get(channel)
