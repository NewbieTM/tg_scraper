import os
from pathlib import Path
from datetime import datetime, timedelta


class DataCleaner:
    def __init__(self, vector_db, storage, retention_days, media_base_path):
        """
        :param vector_db: экземпляр VectorDatabase
        :param storage: экземпляр DataManager
        :param retention_days: сколько дней хранить данные
        :param media_base_path: путь до папки, где лежат медиа (MEDIA_SAVE_PATH)
        """
        self.vector_db = vector_db
        self.storage = storage
        self.retention_days = retention_days
        self.media_base = Path(media_base_path)

    async def clean_all(self):
        """
        Запускает полную очистку:
         1) Векторной БД (по возрасту post['date'])
         2) JSON-хранилища (по полю timestamp)
         3) Медиа-файлов (по именам папок YYYY-MM-DD)
        """
        # 1) Чистим векторную БД
        try:
            self.vector_db.clean_old_data(self.retention_days)
        except Exception as e:
            print(f"❌ Ошибка при чистке VectorDatabase: {e}")

        # 2) Чистим JSON-логи
        try:
            await self.storage.clean_old_data(self.retention_days)
        except Exception as e:
            print(f"❌ Ошибка при чистке JSON-хранилища: {e}")

        # 3) Чистим медиа-файлы
        await self._clean_media_files()

    async def _clean_media_files(self):
        """
        Удаляет старые папки вида: media/<channel_name>/<YYYY-MM-DD>/...
        Поскольку мы храним медиа внутри папок с названием даты, просто сравниваем YYYY-MM-DD с cutoff.
        """
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)

        # Проходим по всем поддиректориям каждого канала
        if not self.media_base.exists():
            return

        for channel_dir in self.media_base.iterdir():
            if not channel_dir.is_dir():
                continue
            # channel_dir = media/<channel_name>
            for date_dir in channel_dir.iterdir():
                # date_dir = media/<channel_name>/<YYYY-MM-DD>
                try:
                    dir_date = datetime.fromisoformat(date_dir.name)
                except Exception:
                    # Если название папки не в формате YYYY-MM-DD, пропускаем
                    continue

                if dir_date < cutoff_date:
                    # Удаляем всю папку date_dir целиком
                    try:
                        # Рекурсивное удаление всех файлов
                        for root, dirs, files in os.walk(date_dir, topdown=False):
                            for file in files:
                                file_path = Path(root) / file
                                try:
                                    file_path.unlink()
                                except Exception:
                                    pass
                            for d in dirs:
                                try:
                                    (Path(root) / d).rmdir()
                                except Exception:
                                    pass
                        # Удаляем саму папку date_dir
                        date_dir.rmdir()
                        print(f"🗑️ Удалена папка медиа: {date_dir}")
                    except Exception as e:
                        print(f"❌ Ошибка при удалении медиа-папки {date_dir}: {e}")

            # Если после этого у channel_dir пусто, удаляем и его
            try:
                if not any(channel_dir.iterdir()):
                    channel_dir.rmdir()
                    print(f"🗑️ Удалена пустая папка канала: {channel_dir}")
            except Exception as e:
                print(f"❌ Ошибка при удалении папки канала {channel_dir}: {e}")
