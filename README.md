# rag_generator
import csv files to clickhouse and generate rag index

1. Проверка наличия GPU на хосте для Docker
Выполните на хосте (Debian 13):

bash
# Проверка драйверов NVIDIA
nvidia-smi

# Проверка nvidia-docker2
docker run --rm --gpus all nvidia/cuda:12.1-runtime nvidia-smi
Если последняя команда показывает GPU – всё готово. Если нет – установите nvidia-docker2:

bash
sudo apt install -y nvidia-docker2
sudo systemctl restart docker

# запуск индексации 
from reindex_gpu import reindex_data_gpu