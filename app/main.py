# main.py
import asyncio
import argparse
from pathlib import Path
from loguru import logger
import torch
from parsers import PARSERS
from click_async import ClickHouseManager
from beverage_repository import BeverageRepository
from import_service import ImportService
from reindex_gpu import reindex_data_gpu  # Наш новый модуль


async def main():
    # 1. Парсинг аргументов для выбора режима
    parser = argparse.ArgumentParser(description="Beverage Data Tool")
    parser.add_argument(
        '--mode', choices=['import', 'reindex'], default='reindex',
        help='Mode: import (from CSV) or reindex (CH to CH)'
    )
    args = parser.parse_args()

    # 2. Проверка окружения
    data_dir = Path("/app/data")
    cache_dir = Path("/app/cache")
    onnx_dir = Path("/app/onnx")

    if torch.cuda.is_available():
        logger.success(f"🚀 CUDA is available. Using GPU: {torch.cuda.get_device_name(0)}")
    else:
        logger.warning("⚠️ CUDA not available. Process will be slow on CPU.")

    # 3. Инициализация ClickHouse
    ch_manager = ClickHouseManager()
    client = await ch_manager.connect()
    repo = BeverageRepository(client)

    try:
        if args.mode == 'import':
            logger.info("--- Starting CSV Import Mode ---")
            await repo.ensure_table()
            importer = ImportService(repo, data_dir=data_dir, cache_dir=cache_dir)
            for name, parser in PARSERS.items():
                if (Path("/app/data") / name).exists():
                    await importer.import_file(name, parser)
            importer.embedder.unload()
            logger.info("All imports completed, GPU model unloaded")

        elif args.mode == 'reindex':
            logger.info("--- Starting ClickHouse Reindexing Mode (GPU) ---")
            # Запуск нашей новой логики с расширенным индексом
            # Передаем клиент и путь к кэшу для ImportEmbedding
            await reindex_data_gpu(client, str(onnx_dir))

    except Exception as e:
        logger.exception(f"Critical error in main: {e}")
    finally:
        await ch_manager.close()
        logger.info("Connections closed.")


if __name__ == "__main__":
    asyncio.run(main())
