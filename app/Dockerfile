# ==================== СТАДИЯ 1: СБОРКА ЗАВИСИМОСТЕЙ ====================
FROM nvidia/cuda:12.1-runtime-ubuntu22.04 AS builder
RUN apt update && apt install -y \
    python3.12 python3-pip git cmake build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --prefix=/install -r requirements.txt

# ==================== СТАДИЯ 2: ФИНАЛЬНЫЙ ОБРАЗ ====================
FROM nvidia/cuda:12.1-runtime-ubuntu22.04
RUN apt update && apt install -y \
    python3.12 python3-pip libgomp1 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /install /usr/local
COPY . .
CMD ["python3", "main.py"]