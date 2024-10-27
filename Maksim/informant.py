import pika
import json

class Informant:
    def __init__(self, informant_name):
        self.informant_name = informant_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel.queue_declare(queue=self.informant_name, durable=True)
        self.channel.queue_bind(exchange='all_agents', queue=self.informant_name)

        self.run_consuming()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        # в поле text заглушка
        if operation == 'available allocator':
            response = {'queue': self.informant_name, 'operation': 'available allocator', 'text': 'distributor1'}
            self.channel.basic_publish(
                exchange='',
                routing_key=agent,
                body=json.dumps(response)
            )
            print(f"Sent available allocators to {data['queue']}")

        # Отправка подтверждения серверу статуса активности.
        elif agent == 'server' and operation == 'activity check':
            response = {'queue': self.informant_name, 'operation': 'activity confirmation', 'text': 'ONLINE'}
            self.channel.basic_publish(
                exchange='',
                routing_key='server',
                body=json.dumps(response)
            )

    def run_consuming(self):
        self.channel.basic_consume(
            queue=self.informant_name,
            auto_ack=True,
            on_message_callback=self.callback
        )
        print(f"{self.informant_name} started listening for requests.")
        self.channel.start_consuming()
