import pika
import json

class AddServer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Подключение.
    channel = connection.channel()  # Создание канала.

    @staticmethod
    def callback(channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        print(f'Request from {agent}')
        if operation == 'available informant':
            data = {'queue': 'server', 'operation': 'available informant', 'text': 'informant1'}
            channel.basic_publish(
                exchange='',
                routing_key=agent,
                body=json.dumps(data),  # Отправляет заказчику свободного информатора.
            )

    def __init__(self):
        self.channel.queue_declare(queue='server', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.run_consuming()

    def run_consuming(self):
        """Начинает принимать сообщения."""
        self.channel.basic_consume(
            queue='server',
            auto_ack=True,  # Автоматического подтверждения выполненной задачи.
            on_message_callback=self.callback
        )
        self.channel.start_consuming()  # Старт потребления (бесконечный цикл).

AddServer()