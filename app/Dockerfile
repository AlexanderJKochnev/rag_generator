FROM python:3.10-slim

# Ставим зависимости для работы и сборки бинарных модулей (Clickhouse)
RUN apt-get update && apt-get install -y \
    libgomp1 \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. Устанавливаем Torch ПРАВИЛЬНО (ровно под CUDA 12.4)
RUN pip3 install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cu124

# 2. Устанавливаем остальные зависимости
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Проброс GPU для Debian 13
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV NVIDIA_DISABLE_REQUIRE=true

CMD ["python3", "main.py", "--mode reindex"]
