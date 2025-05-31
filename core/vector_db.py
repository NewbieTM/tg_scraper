import faiss
import numpy as np
import pickle
from pathlib import Path
from datetime import datetime, timedelta
from sentence_transformers import SentenceTransformer


class VectorDatabase:
    def __init__(
        self,
        model_name: str = 'intfloat/multilingual-e5-large-instruct',
        threshold: float = 0.85,
        db_file: str = 'db/vector_db.pkl'
    ):
        """
        :param model_name: имя модели SentenceTransformer
        :param threshold: порог косинусной схожести (после нормировки) для отбраковки дубликатов
        :param db_file: путь до .pkl-файла, в котором храним {'posts': [...], 'index': serialized_index}
        """
        self.model = SentenceTransformer(model_name)
        self.threshold = threshold
        self.db_file = Path(db_file)
        self.posts = []     # Список словарей: каждый содержит все поля post + 'embedding': List[float]
        self.index = None   # FAISS-индекс (IndexFlatIP на нормированных векторах)
        self._load_db()

    def _load_db(self):
        """
        Если db_file существует, загружаем self.posts и десериализуем self.index.
        """
        if self.db_file.exists():
            try:
                with open(self.db_file, 'rb') as f:
                    data = pickle.load(f)
                self.posts = data.get('posts', [])
                idx_bytes = data.get('index')
                if idx_bytes:
                    self.index = faiss.deserialize_index(idx_bytes)
                else:
                    self.index = None
            except Exception as e:
                print(f"❌ Ошибка при загрузке VectorDatabase: {e}")
                self.posts = []
                self.index = None
        else:
            self.posts = []
            self.index = None

    def _save_db(self):
        """
        Сериализует self.posts и self.index в файл self.db_file.
        """
        try:
            data = {
                'posts': self.posts,
                'index': faiss.serialize_index(self.index) if self.index else None
            }
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.db_file, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"❌ Ошибка при сохранении VectorDatabase: {e}")

    def text_to_embedding(self, text: str) -> np.ndarray:
        """
        Превращает текст (str) в нормированный эмбеддинг (numpy array float32).
        Если текст пуст, возвращаем вектор нулей.
        """
        if not text:
            # Если нет текста, возвращаем вектор нулей нужной размерности
            dim = self.model.get_sentence_embedding_dimension()
            return np.zeros((dim,), dtype=np.float32)
        vec = self.model.encode([text])[0].astype(np.float32)
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return vec

    def is_duplicate(self, embedding: np.ndarray) -> bool:
        """
        Проверяет, является ли данный эмбеддинг дубликатом: ищем ближайший у себя же,
        если косинусная схожесть (inner product у нормированных векторов) > threshold, считаем дубликатом.
        """
        if self.index is None or not self.posts:
            return False
        # FAISS IndexFlatIP ожидает float32
        query = np.expand_dims(embedding.astype(np.float32), axis=0)
        distances, _ = self.index.search(query, 1)  # возвращает значение inner product
        # distances[0][0] — ближайшее значение inner product
        return distances[0][0] > self.threshold

    def init_index(self, dimension: int):
        """
        Создаёт FAISS-индекс IndexFlatIP(dimension) для последующих inner product (cosine после нормировки).
        """
        self.index = faiss.IndexFlatIP(dimension)

    def add_post(self, post: dict) -> bool:
        """
        Добавляет пост в базу, если он не дубликат.
        :param post: словарь с полями { 'channel', 'date', 'text', 'id', 'media': [...] }
        :return: True, если добавлено, False, если дубликат.
        """
        # Генерируем эмбеддинг уже в момент добавления
        embedding = self.text_to_embedding(post['text'])
        if self.index is None:
            self.init_index(embedding.shape[0])

        # Проверка на дубликаты
        if self.is_duplicate(embedding):
            return False

        # Сохраняем копию поста вместе с эмбеддингом (list[float])
        post_copy = post.copy()
        post_copy['embedding'] = embedding.tolist()
        self.posts.append(post_copy)

        # Добавляем в FAISS-индекс
        self.index.add(np.expand_dims(embedding, axis=0))

        # Сохраняем БД на диск
        self._save_db()
        return True

    def clean_old_data(self, retention_days: int):
        """
        Удаляет из self.posts те записи, у которых date < now - retention_days.
        Затем пересоздаёт FAISS-индекс из оставшихся эмбеддингов.
        """
        cutoff = datetime.now() - timedelta(days=retention_days)
        filtered = []
        for p in self.posts:
            try:
                dt = datetime.fromisoformat(p['date'])
                if dt > cutoff:
                    filtered.append(p)
            except Exception:
                # Если неверный формат даты, оставляем на всякий случай
                filtered.append(p)
        self.posts = filtered

        # Пересоздаём индекс
        if self.posts:
            dim = len(self.posts[0]['embedding'])
            self.init_index(dim)
            all_embeds = np.array(
                [np.array(p['embedding'], dtype=np.float32) for p in self.posts],
                dtype=np.float32
            )
            self.index.add(all_embeds)
        else:
            self.index = None

        self._save_db()
