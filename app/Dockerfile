# --- ЭТАП 1: Сборка (Builder) ---
FROM nvidia/cuda:12.4.1-devel-ubuntu22.04 AS builder

RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .

# Собираем все пакеты в локальную директорию /build/wheels
# Это позволит избежать повторной компиляции на втором этапе
RUN pip3 wheel --no-cache-dir --wheel-dir=/build/wheels -r requirements.txt


# --- ЭТАП 2: Финальный образ (Runtime) ---
# Используем runtime версию — она значительно меньше devel версии
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3 python3-pip libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем только собранные "колеса" из первого этапа
COPY --from=builder /build/wheels /wheels
COPY requirements.txt .

# Устанавливаем пакеты из готовых файлов (без компиляции!)
RUN pip3 install --no-cache-dir /wheels/*

COPY . .

CMD ["python3", "main.py"]
