# support.clickhouse.import_service.services.import_service.py
# import_service/import_service.py
import hashlib
import time
import pandas as pd
from pathlib import Path
from typing import Callable, Dict, Any, Optional
from loguru import logger
from embedding import ImportEmbedding
from beverage_repository import BeverageRepository
from schemas import BeverageCreate, BeverageCategory
# from parsers import PARSERS  # если нужно, но не обязательно



class ImportService:
    def __init__(self, repo: BeverageRepository, data_dir: Path, cache_dir: Path):
        """
        Сервис для импорта CSV файлов в ClickHouse.

        :param repo: Репозиторий для работы с БД
        :param data_dir: Путь к папке с CSV файлами (обычно /app/data)
        :param cache_dir: Путь к кэшу Hugging Face (обычно /app/cache)
        """
        self.repo = repo
        self.data_dir = Path(data_dir)
        self.embedder = ImportEmbedding(cache_dir=str(cache_dir))

    @staticmethod
    def _compute_file_hash(file_path: Path) -> str:
        """Вычисляет MD5 хэш содержимого файла."""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    async def import_file(self, file_name: str, parser_func: Callable) -> Dict[str, Any]:
        """
        Импортирует один CSV файл.

        :param file_name: Имя файла (например, 'beer_data.csv')
        :param parser_func: Функция-парсер для данного типа CSV
        :return: Словарь с результатом импорта
        """
        file_path = self.data_dir / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # 1. Проверка дубликата по хэшу
        file_hash = self._compute_file_hash(file_path)
        if await self.repo.file_exists(file_hash):
            logger.info(f"Skipping {file_name} – already imported (hash: {file_hash[:8]}...)")
            return {"file": file_name, "status": "skipped", "rows": 0}

        logger.info(f"Importing {file_name}...")
        t0 = time.time()
        df = pd.read_csv(file_path)
        t1 = time.time()
        total_rows = len(df)
        logger.info(f"Read {total_rows} rows from {file_name}, {t1 - t0:.2f}s")

        # 2. Парсинг строк и подготовка текстов для эмбеддингов
        parsed_data = []
        texts_for_embedding = []
        for idx, row in df.iterrows():
            try:
                data = parser_func(row, str(file_path))
                parsed_data.append(data)
                texts_for_embedding.append(f"{data['name']}. {data['description']}"[:1000])
            except Exception as e:
                logger.error(f"Parse error in {file_name} at row {idx}: {e}")
                continue
        t2 = time.time();
        logger.info(f"Parsing: {t2 - t1:.2f}s")
        if not parsed_data:
            logger.warning(f"No valid rows in {file_name}")
            return {"file": file_name, "status": "failed", "rows": 0}

        # 3. Генерация эмбеддингов (GPU-модель)
        logger.info(f"Generating embeddings for {len(parsed_data)} rows...")
        embeddings = self.embedder.encode(texts_for_embedding)
        t3 = time.time()
        logger.info(f"Embedding: {t3 - t2:.2f}s")
        # 4. Сохранение в ClickHouse
        inserted = 0
        for data, emb in zip(parsed_data, embeddings):
            # Создаём Pydantic-модель для вставки
            t4 = time.time()
            # from schemas import BeverageCreate, BeverageCategory
            # Преобразуем категорию (строка -> enum)
            try:
                category = BeverageCategory(data['category'].lower())
            except ValueError:
                logger.warning(f"Unknown category '{data['category']}', using 'other'")
                category = BeverageCategory.OTHER

            beverage = BeverageCreate(
                name=data['name'],
                description=data['description'],
                category=category,
                country=data.get('country'),
                brand=data.get('brand'),
                abv=data.get('abv'),
                price=data.get('price'),
                rating=data.get('rating'),
                attributes=data.get('attributes', {})
            )
            await self.repo.create(beverage, file_hash, str(file_path), emb)
            inserted += 1
            if inserted % 100 == 0:
                t5 = time.time()
                logger.info(f"inserted records {inserted}: {t5 - t4:.2f}s")

        # 5. Обязательная выгрузка GPU-модели после импорта
        self.embedder.unload()

        logger.success(f"✅ Imported {inserted} rows from {file_name}")
        return {"file": file_name, "status": "imported", "rows": inserted}