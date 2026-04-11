#!/usr/bin/env python3
import asyncio
from loguru import logger
from pathlib import Path
from click_async import ClickHouseManager
from parsers import PARSERS
from import_service import ImportService
from beverage_repository import BeverageRepository
import torch


async def main():
    logger.info(f"CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        logger.info(f"Current device: {torch.cuda.get_device_name(0)}")
        logger.info(f"CUDA version: {torch.version.cuda}")
    else:
        logger.info("CUDA is NOT available. Check driver/toolkit.")
    ch = ClickHouseManager()  # host="clickhouse", port=8123, user="default", password="")
    try:
        await ch.connect()
        repo = BeverageRepository(ch.client)
        await repo.ensure_table()
        importer = ImportService(repo, data_dir=Path("/app/data"), cache_dir=Path("/app/cache"))
        for name, parser in PARSERS.items():
            if (Path("/app/data") / name).exists():
                await importer.import_file(name, parser)
        importer.embedder.unload()
        logger.info("All imports completed, GPU model unloaded")
    finally:
        await ch.close()

if __name__ == "__main__":
    asyncio.run(main())
