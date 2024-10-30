import pika
import json
import time
import threading

class Informant:
    def __init__(self, informant_name):
        self.informant_name = informant_name
        self.active_agents = []

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        connection2 = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()
        self.channel2 = connection2.channel()

        self.channel.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel2.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel.queue_declare(queue=self.informant_name, durable=True)
        self.channel.queue_bind(exchange='all_agents', queue=self.informant_name)

        t1 = threading.Thread(target=self.collect_allocators)
        t1.start()
        t2 = threading.Thread(target=self.run_consuming)
        t2.start()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        

        if  operation == 'activity check':
            response = {'queue': self.informant_name, 'operation': 'activity confirmation', 'text': 'ONLINE'}
            self.channel.basic_publish(exchange='', routing_key='server', body=json.dumps(response))

        elif operation == 'available allocator':
            allocators = [agent for agent in self.active_agents if agent.startswith('distributor')]
            if allocators:
                allocator = allocators[0]
            else:
                allocator = 'no active allocators'
            response = {
                'queue': self.informant_name,
                'operation': 'available allocator',
                'text': allocator
            }
            self.channel.basic_publish(exchange='', routing_key=agent, body=json.dumps(response))
            print(f"Sent available allocators to {data['queue']}")

        elif operation == 'activity confirmation':
            if agent not in self.active_agents:
                self.active_agents.append(agent)
                print(f"Added {agent} to active allocators.")

    def collect_allocators(self):
        while True:
            self.active_agents = []
            data = {'queue': self.informant_name, 'operation': 'activity check', 'text': 'confirm activity status'}
            self.channel2.basic_publish(
                exchange='all_agents',
                routing_key='',
                body=json.dumps(data)
                )
            time.sleep(5)
            print(f'-- Active agents: {self.active_agents}')

    def run_consuming(self):
        self.channel.basic_consume(queue=self.informant_name, auto_ack=True, on_message_callback=self.callback)
        print(f"{self.informant_name} started listening for requests.")
        self.channel.start_consuming()

Informant('informant1')
