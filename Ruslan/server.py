import pika
import json
import time
import threading
import amqpstorm

class AddServer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Подключение.
    channel = connection.channel()  # Создание канала.

    def __init__(self):
        self.active_agents = [] # Список активных агентов.
        self.channel.queue_declare(queue='server', durable=True)
        self.channel.basic_qos(prefetch_count=1)

        # https://stackoverflow.com/questions/65423312/pika-pop-from-an-empty-queue
        # Собирает адреса активных агентов.
        conn = amqpstorm.Connection('localhost', 'guest', 'guest')
        threading.Thread(target=self.get_active_agents, kwargs={'conn': conn}).start()
        self.run_consuming()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        print(f'Request from {agent}')

        # Возврат активного информатора заказчику.
        if agent.startswith('customer') and operation == 'available informant':
            informants = [agent for agent in self.active_agents if agent.startswith('informant')]
            if informants:
                informant = informants[0]
            else:
                informant = 'no active informants'
            response = {'queue': 'server', 'operation': 'available informant', 'text': informant}
            channel.basic_publish(
                exchange='',
                routing_key=agent,
                body=json.dumps(response)
            )

        # Подтверждение активности от агентов.
        elif operation == 'activity confirmation':
            self.active_agents.append(agent)


    def run_consuming(self):
        """Начинает принимать сообщения."""
        self.channel.basic_consume(
            queue='server',
            auto_ack=True,  # Автоматического подтверждения выполненной задачи.
            on_message_callback=self.callback
        )
        self.channel.start_consuming()  # Старт потребления (бесконечный цикл).

    def get_active_agents(self, conn: amqpstorm.Connection):
        """Получить активных агентов"""
        with conn.channel():
            while True:
                self.active_agents = []  # Обнуление списка активных агентов.
                data = {'queue': 'server', 'operation': 'activity check', 'text': 'confirm activity status'}
                self.channel.basic_publish(
                    exchange='all_agents',
                    routing_key='',
                    body=json.dumps(data),
                )
                time.sleep(5)
                print(f'-- Active agents {self.active_agents}')

AddServer()