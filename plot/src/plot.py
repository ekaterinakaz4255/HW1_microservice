import time
import pandas as pd
import pika
import json
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очередь absolute_error
    channel.queue_declare(queue='absolute_error')

    # Создаём функцию callback для обработки данных из очереди absolute_error
    def callback(ch, method, properties, body):
        message = json.loads(body)
        if message == True:
            print('Новое значение абсолютной ошибки получено')

            # Создаем график,как только получено новое значение абсолютной ошибки
            log_file = '/usr/src/app/metric/metric_log.csv'
            if os.path.exists(log_file):
                df = pd.read_csv(log_file)
                errors = df['absolute_error']

                # Построение гистограммы
                plt.figure()
                sns.histplot(errors, bins=30, kde=True, color='orange')
                plt.title('Error Distribution')
                plt.xlabel('Absolute Error')
                plt.ylabel('Count')
                                
                # Сохранение графика
                plt.savefig('/usr/src/app/logs/error_distribution.png')
                plt.close()
                print('График сохранён')
            else:
                print('Файл логов не найден')

        else:
            print(f'Очередь {method.routing_key} пуста, ожидание новых данных')

    # Извлекаем сообщение из очереди absolute_error
    channel.basic_consume(
        queue='absolute_error',
        on_message_callback=callback,
        auto_ack=True
    )
    channel.start_consuming()

except Exception as e:
    print(f'Не удалось подключиться к очереди, {e}')