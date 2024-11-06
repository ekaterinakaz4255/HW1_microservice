import time
from datetime import datetime
import random
import pika
import json
import numpy as np
from sklearn.datasets import load_diabetes

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Генерация уникального идентификатора
        message_id = datetime.timestamp(datetime.now())

        # Загружаем датасет о диабете
        x, y = load_diabetes(return_X_y=True)
        # Формируем случайный индекс строки
        random_row = np.random.randint(0, x.shape[0]-1)

        # Создаём очередь y_true
        channel.queue_declare(queue='y_true')
        # Создаём очередь features
        channel.queue_declare(queue='features')

        # Создание сообщений
        message_y_true = {
            'id': message_id,
            'body': y[random_row].tolist()
        }

        message_features = {
            'id': message_id,
            'body': x[random_row].tolist()
        }

        # Публикуем сообщение в очередь y_true
        channel.basic_publish(exchange='',
                              routing_key='y_true',
                              body=json.dumps(message_y_true))
        print(
            f'Сообщение {message_id} с правильным ответом отправлено в очередь')

        # Публикуем сообщение в очередь features
        channel.basic_publish(exchange='',
                              routing_key='features',
                              body=json.dumps(message_features))
        print(
            f'Сообщение {message_id} с вектором признаков отправлено в очередь')

        # Задержка
        time.sleep(30)

        # Закрываем подключение
        connection.close()
    except Exception as e:
        print(f'Не удалось подключиться к очереди, {e}')
        time.sleep(5)
