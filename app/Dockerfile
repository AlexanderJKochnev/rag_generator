# ==================== СТАДИЯ 1: СБОРКА ЗАВИСИМОСТЕЙ ====================
FROM nvidia/cuda:12.5.1-runtime-ubuntu24.04 AS builder
# FROM nvidia/cuda:12.1-runtime-ubuntu22 AS builder
RUN apt update && apt install -y \
    python3.12 python3-pip git cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Устанавливаем ВСЕ зависимости (включая clickhouse-connect) в отдельную директорию
RUN pip3 install --no-cache-dir --prefix=/install -r requirements.txt

# ==================== СТАДИЯ 2: ФИНАЛЬНЫЙ ОБРАЗ ====================
# FROM nvidia/cuda:12.1-runtime-ubuntu22.04
FROM nvidia/cuda:12.5.1-runtime-ubuntu24.04
RUN apt update && apt install -y \
    python3.12 python3-pip git cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Устанавливаем ВСЕ зависимости (включая clickhouse-connect) в отдельную директорию
RUN pip3 install --no-cache-dir --prefix=/install -r requirements.txt

RUN apt update && apt install -y \
    python3.12 python3-pip git cmake build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .

# Устанавливаем ВСЕ зависимости (включая clickhouse-connect) в отдельную директорию
RUN pip3 install --no-cache-dir --prefix=/install -r requirements.txt