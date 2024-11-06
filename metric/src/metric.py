import pika
import json
import pandas as pd
import os
import time

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
    # Объявляем очередь absolute_error
    channel.queue_declare(queue='absolute_error')

    # Создаём функцию callback для обработки данных из очереди y_true
    def callback_true(ch, method, properties, body):
        message = json.loads(body)
        id = message['id']
        value = message['body']
        answer_string = f'Из очереди {method.routing_key} получено значение {value}'
        print(answer_string)

        log_file = '/usr/src/app/logs/metric_log.csv'
        # Если файл логов существует, читаем его в DataFrame, иначе создаем новый
        if os.path.exists(log_file):
            df = pd.read_csv(log_file)
        else:
            df = pd.DataFrame(
                columns=['id', 'y_true', 'y_pred', 'absolute_error'])

        # Проверяем, есть ли id в DataFrame, если нет, добавляем новую строку
        if id not in df['id'].values:
            new_row = pd.DataFrame({'id': [id], 'y_true': [value], 'y_pred': [
                                   None], 'absolute_error': [None]})
            df = pd.concat([df, new_row], ignore_index=True)
        else:
            df.loc[df['id'] == id, 'y_true'] = value

        # Рассчитываем absolute_error, если оба значения y_true и y_pred присутствуют
        if pd.notnull(df.loc[df['id'] == id, 'y_true'].values[0]) and pd.notnull(df.loc[df['id'] == id, 'y_pred'].values[0]):
            df.loc[df['id'] == id, 'absolute_error'] = abs(
                df.loc[df['id'] == id, 'y_true'] - df.loc[df['id'] == id, 'y_pred'])
            # Публикуем сообщение в очередь absolute_error
            channel.basic_publish(exchange='',
                                  routing_key='absolute_error',
                                  body=json.dumps(True))
            print('Абсолютная ошибка рассчитана и отправлена в очередь')

        # Записываем DataFrame в CSV
        df.to_csv(log_file, index=False)

    # Создаём функцию callback для обработки данных из очереди y_pred
    def callback_pred(ch, method, properties, body):
        message = json.loads(body)
        id = message['id']
        value = message['body']
        answer_string = f'Из очереди {method.routing_key} получено значение {value}'
        print(answer_string)

        log_file = '/usr/src/app/logs/metric_log.csv'
        # Если файл логов существует, читаем его в DataFrame, иначе создаем новый
        if os.path.exists(log_file):
            df = pd.read_csv(log_file)
        else:
            df = pd.DataFrame(
                columns=['id', 'y_true', 'y_pred', 'absolute_error'])

        # Проверяем, есть ли id в DataFrame, если нет, добавляем новую строку
        if id not in df['id'].values:
            new_row = pd.DataFrame({'id': [id], 'y_true': [None], 'y_pred': [
                                   value], 'absolute_error': [None]})
            df = pd.concat([df, new_row], ignore_index=True)
        else:
            df.loc[df['id'] == id, 'y_pred'] = value

        # Рассчитываем absolute_error, если оба значения y_true и y_pred присутствуют
        if pd.notnull(df.loc[df['id'] == id, 'y_true'].values[0]) and pd.notnull(df.loc[df['id'] == id, 'y_pred'].values[0]):
            df.loc[df['id'] == id, 'absolute_error'] = abs(
                df.loc[df['id'] == id, 'y_true'] - df.loc[df['id'] == id, 'y_pred'])

            # Публикуем сообщение в очередь absolute_error
            channel.basic_publish(exchange='',
                                  routing_key='absolute_error',
                                  body=json.dumps(True))
            print('Абсолютная ошибка рассчитана и отправлена в очередь')

        df.to_csv(log_file, index=False)

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback_true,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback_pred,
        auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
    
except Exception as e:
    print(f'Не удалось подключиться к очереди, {e}')
    
    # Задержка
    time.sleep(5)
