from pickle import REDUCE

import pika
import time
import json

class Render:

    def __init__(self,queue_name,render_name,host='localhost'):
        self.render_name = render_name
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)



    def connect(self):
        self.channel.basic_consume(queue=self.queue_name,
                                   on_message_callback=self.callback)

    def callback(self, ch, method, properties, body):
        data = json.loads(body)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=f'{self.render_name} is connected.')
        print(f'{self.render_name} received task : ...')
        time.sleep(data.get('duration'))
        result = f'Result is ...'
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=json.dumps({'task':'completed',
                                                    'result':result}))
        ch.basic_ack(deliver_tag=method.delivery_tag)

    def start_consume(self):
        self.channel.start_consuming()

if __name__ == '__main__':
    rend = Render('distributor','pc1')
    rend.start_consume()