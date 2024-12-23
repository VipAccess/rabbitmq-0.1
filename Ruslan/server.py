import pika
import json
import time
import threading

class AddServer:

    def __init__(self):
        self.active_agents = []  # Список активных агентов.
        # Подключение.
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        connection2 = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()  # Создание канала.
        self.channel2 = connection2.channel()  # Создание канала.

        self.channel.queue_declare(queue='server', durable=True)
        self.channel2.queue_declare(queue='server', durable=True)
        self.channel.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel2.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)

        t1 = threading.Thread(target=self.get_active_agents)
        # t1.setDaemon(True)
        t1.start()
        t2 = threading.Thread(target=self.run_consuming)
        # t2.setDaemon(True)
        t2.start()

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
            self.channel.basic_publish(

                exchange='',
                routing_key=agent,
                body=json.dumps(response)
            )

        # Подтверждение активности от агентов.
        elif operation == 'activity confirmation':
            if agent not in self.active_agents:
                self.active_agents.append(agent)

    def run_consuming(self):
        """Начинает принимать сообщения."""
        self.channel.basic_consume(
            queue='server',
            auto_ack=True,  # Автоматического подтверждения выполненной задачи.
            on_message_callback=self.callback
        )
        self.channel.start_consuming()  # Старт потребления (бесконечный цикл).

    def get_active_agents(self):
        """Получить активных агентов"""
        while True:
            self.active_agents = []  # Обнуление списка активных агентов.
            data = {'queue': 'server', 'operation': 'activity check', 'text': 'confirm activity status'}
            self.channel2.basic_publish(
                exchange='all_agents',
                routing_key='',
                body=json.dumps(data),
            )
            time.sleep(5)
            print(f'-- Active agents {self.active_agents}')


AddServer()