import pika
import json
import time
import threading

class Distributor:
    def __init__(self, distributor_name):
        self.distributor_name = distributor_name

        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = connection.channel()
        
        self.channel.queue_declare(queue=self.distributor_name, durable=True)
        self.channel.exchange_declare(exchange='all_agents', exchange_type='fanout', durable=True)
        self.channel.queue_bind(exchange='all_agents', queue=self.distributor_name)

        t1 = threading.Thread(target=self.run_consuming)
        t1.start()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']

        if operation == 'submit_task':
            task_duration = data.get('text', 0)
            print(f"Received task with duration {task_duration} seconds.")
            self.execute_task(task_duration)

        elif operation == 'activity check':
            response = {
                'queue': self.distributor_name,
                'operation': 'activity confirmation',
                'text': 'ACTIVE'
            }
            self.channel.basic_publish(
                exchange='',
                routing_key=agent,
                body=json.dumps(response)
            )

    def execute_task(self, duration):
        print(f"{self.distributor_name} is executing task for {duration} seconds.")
        time.sleep(duration)
        print(f"{self.distributor_name} completed the task.")

    def run_consuming(self):
        self.channel.basic_consume(queue=self.distributor_name, auto_ack=True, on_message_callback=self.callback)
        print(f"{self.distributor_name} started listening for tasks and activity checks.")
        self.channel.start_consuming()

Distributor('distributor1')
