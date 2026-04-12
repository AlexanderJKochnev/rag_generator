# app.support.clickhouse.reindex_gpu.py
import json
import time
from loguru import logger
# from app.support.clickhouse.service_gpu import ImportEmbedding  # Твой класс из примера
from embedding import ImportEmbedding


async def reindex_data_gpu(ch_client, onnx_dir: str):
    source_table = "beverages_rag"
    target_table = "beverages_rag_v2"

    # Инициализируем твой GPU-эмбеддер
    embedder = ImportEmbedding(cache_dir=onnx_dir)

    # Тот самый расширенный индекс, который мы утвердили
    def create_rag_string(row):
        name = str(row.get('name', ''))
        cat = str(row.get('category', ''))
        header = f"Product: {name}. Category: {cat}."
        if row.get('brand'):
            header += f" Brand: {row['brand']}."

        specs = ""
        if row.get('country'):
            specs += f" Country: {row['country']}."
        if row.get('abv'):
            specs += f" ABV: {row['abv']}%."

        desc = f" | Description: {row.get('description', '')}"

        attr_str = ""
        if row.get('attributes'):
            try:
                attrs = row['attributes']
                if isinstance(attrs, str):
                    attrs = json.loads(attrs)
                if isinstance(attrs, dict):
                    attr_str = " | Attributes: " + ", ".join([f"{k}: {v}" for k, v in attrs.items()])
            except Exception as e:
                logger.error(f'error 1: {e}')
                pass
        return (header + specs + desc + attr_str)[:1000].strip()

    # Список колонок строго по твоей схеме (кроме embedding)
    columns = ['id', 'name', 'description', 'category', 'country', 'brand', 'abv', 'price', 'rating', 'attributes',
               'file_hash', 'source_file', 'created_at']
    full_columns = columns + ['embedding']

    logger.info(f"🚀 Starting GPU reindexing: {source_table} -> {target_table}")

    try:
        # GPU любит большие пачки. Читаем по 20 000 строк.
        query = f"SELECT {', '.join(columns)} FROM {source_table} WHERE id NOT IN (SELECT id FROM {target_table})"
        settings = {'max_block_size': 20000, 'max_execution_time': 0}

        async with await ch_client.query_column_block_stream(query, settings=settings) as stream:
            total_count = 0

            async for block_columns in stream:
                t_start = time.time()

                # 1. Транспонируем колонки в строки
                block_rows = list(zip(*block_columns))
                rows = [dict(zip(columns, row)) for row in block_rows]

                # 2. Формируем тексты для GPU
                texts = [create_rag_string(r) for r in rows]

                # 3. Генерация эмбеддингов через твой класс (использует GPU)
                # В твоем классе encode уже делает list.tolist()
                embeddings = embedder.encode(texts)

                # 4. Сборка пачки для вставки
                final_batch = []
                for i, row in enumerate(rows):
                    row['embedding'] = embeddings[i]

                    # Фикс JSON для ClickHouse
                    attrs = row.get('attributes')
                    if isinstance(attrs, str):
                        try:
                            row['attributes'] = json.loads(attrs)
                        except Exception as e:
                            logger.error(f'error 1: {e}')
                            row['attributes'] = {}
                    elif attrs is None:
                        row['attributes'] = {}

                    final_batch.append(tuple(row[col] for col in full_columns))

                # 5. Вставка в новую таблицу
                await ch_client.insert(target_table, final_batch, column_names=full_columns)

                total_count += len(rows)
                t_end = time.time()
                logger.info(f"Indexed {total_count} rows. Batch speed: {len(rows) / (t_end - t_start):.1f} rows/s")

        logger.success(f"✅ GPU Reindexing finished! Total: {total_count}")

    except Exception as e:
        logger.exception(f"GPU Reindexing failed: {e}")
        raise e
    finally:
        embedder.unload()  # Освобождаем VRAM
