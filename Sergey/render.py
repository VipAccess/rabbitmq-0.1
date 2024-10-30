from http.client import responses
from pickle import REDUCE

import pika
import time
import json

class Render:

    def __init__(self,render_name):
        self.queue_name = render_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.queue_bind(exchange='all_agents', queue=self.queue_name)

        self.start_consume()


    def callback(self, ch, method, properties, body):
        print(body)
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        text = data['text']

        if operation == 'activity check':
            #ch.basic_ack(deliver_tag=method.delivery_tag)
            response = {'queue': self.queue_name, 'operation': 'activity confirmation', 'text': 'ACTIVE'}
            self.channel.basic_publish(
                exchange='',
                routing_key=agent,
                body=json.dumps(response)
            )
            #ch.basic_ack(deliver_tag=method.delivery_tag)
        elif operation == 'execute_task':
            for i in range(int(text)):
                print(i)
            time.sleep(1)
            response = {'queue': self.queue_name, 'operation': 'task completed', 'text': 'task completed'}
            self.channel.basic_publish(exchange='',
                                       routing_key=agent,
                                       body=json.dumps(response))
            #ch.basic_ack(deliver_tag=method.delivery_tag)

    def start_consume(self):
        self.channel.basic_consume(queue=self.queue_name,
                                   auto_ack=True,
                                   on_message_callback=self.callback)
        self.channel.start_consuming()

Render('render1')