import pika

class AddServer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Подключение.
    channel = connection.channel()  # Создание канала.

    @staticmethod
    def callback(channel, method, properties, body):
        agent, text = body.decode().split('-')
        print(f'Request from {agent}')
        channel.basic_publish(
            exchange='',
            routing_key=agent,
            body='server-info1',  # Отправляет заказчику свободного информатора.
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