# Этап 1: Builder
FROM ubuntu:22.04 AS builder

# Ставим минимум для сборки Python-пакетов
RUN apt-get update && apt-get install -y \
    python3-pip python3-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .

# Собираем все, включая Torch, под CUDA 12.4
# Это гарантирует, что Clickhouse соберется, а Torch будет видеть ваш драйвер 550.163
RUN pip3 wheel --no-cache-dir --wheel-dir=/build/wheels \
    --extra-index-url https://pytorch.org \
    "torch>=2.0.0" && \
    pip3 wheel --no-cache-dir --wheel-dir=/build/wheels -r requirements.txt

# Этап 2: Runtime (Легкий и чистый)
FROM ubuntu:22.04

# Ставим только Python и библиотеки для работы с видеокартой
RUN apt-get update && apt-get install -y \
    python3 python3-pip libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Переносим готовые пакеты
COPY --from=builder /build/wheels /wheels
RUN pip3 install --no-cache-dir /wheels/* && rm -rf /wheels

COPY . .

# Проброс GPU (эти переменные обязательны для работы на Debian 13)
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV NVIDIA_DISABLE_REQUIRE=true

CMD ["python3", "main.py"]
