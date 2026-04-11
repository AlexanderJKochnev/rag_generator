FROM python:3.10-slim

RUN apt-get update && apt-get install -y libgomp1 gcc g++ python3-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. Сначала ставим только torch под нужную версию CUDA (12.4)
RUN pip3 install --no-cache-dir torch --index-url https://pytorch.org

# 2. Потом всё остальное из вашего списка
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV NVIDIA_DISABLE_REQUIRE=true

CMD ["python3", "main.py"]
