FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04
# FROM nvidia/cuda:12.5.1-runtime-ubuntu24.04
# Установка Python и системных зависимостей
RUN apt update && apt install -y \
    python3.12 python3-pip libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем requirements.txt и удаляем из него строку с clickhouse-connect (если есть)
COPY requirements.txt .
# RUN sed -i '/clickhouse-connect/d' requirements.txt

# Устанавливаем все зависимости, кроме clickhouse-connect
# RUN pip3 install --no-cache-dir --prefix=/install -r requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt
# RUN pip3 install --no-cache-dir -r requirements.txt
# RUN pip install --user --no-cache-dir -r requirements.txt
# Отдельно устанавливаем clickhouse-connect (последний слой, кэшируется отдельно)
# RUN pip3 install --no-cache-dir --prefix=/install "clickhouse-connect[async]==0.12.0rc1"
RUN pip3 install --no-cache-dir --break-system-packages "clickhouse-connect[async]==0.12.0rc1"

# Копируем код приложения
COPY . .

CMD ["python3", "main.py"]
