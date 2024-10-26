import pika
import json

class AddServer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Подключение.
    channel = connection.channel()  # Создание канала.

    def __init__(self):
        self.channel.queue_declare(queue='server', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.run_consuming()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        print(f'Request from {agent}')

        # Проверка активных информаторов.
        if agent.startswith('customer') and operation == 'available informant':
            self.get_active_informants()

        # Подтверждение активности от информатора.
        elif agent.startswith('informant') and operation == 'activity confirmation':
            print(f'-- {agent} active')
            # data = {'queue': 'server', 'operation': 'available informant', 'text': 'informant1'}
            # channel.basic_publish(
            #     exchange='',
            #     routing_key=agent,
            #     body=json.dumps(data),  # Отправляет заказчику свободного информатора.
            # )

    def run_consuming(self):
        """Начинает принимать сообщения."""
        self.channel.basic_consume(
            queue='server',
            auto_ack=True,  # Автоматического подтверждения выполненной задачи.
            on_message_callback=self.callback
        )
        self.channel.start_consuming()  # Старт потребления (бесконечный цикл).

    def get_active_informants(self):
        """Получить активных информаторов"""
        data = {'queue': 'server', 'operation': 'activity check', 'text': 'confirm activity status'}
        self.channel.basic_publish(
            exchange='only_informants',
            routing_key='',
            body=json.dumps(data),
        )

AddServer()