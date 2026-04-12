# индексация новой таблицы rag
import asyncio
from click_async import ClickHouseManager
from reindex_gpu import reindex_data_gpu
# from app.support.clickhouse.reindex_logic import reindex_data  # твой код выше


async def main():
    manager = ClickHouseManager()
    client = await manager.connect()
    try:
        # await reindex_data(client)
        await reindex_data_gpu(client, cache_dir="/app/cache")
    finally:
        await manager.close()

if __name__ == "__main__":
    asyncio.run(main())
