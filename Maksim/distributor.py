import pika
import json
import time

class Distributor:
    def __init__(self, distributor_name):
        self.distributor_name = distributor_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        self.channel.queue_declare(queue=self.distributor_name, durable=True)

        self.run_consuming()

    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        print(data)
        operation = data['operation']
        
        # Обработка запроса на задачу
        if operation == 'assign task':
            task_duration = data.get('task_duration', 0)
            print(f"Received task with duration {task_duration} seconds.")
            
            # Разделение задачи между исполнителями(тоже пока заглушка)
            num_executors = 3
            part_duration = task_duration / num_executors
            
            # Заглушка для распределения задач между исполнителями
            for i in range(1, num_executors + 1):
                executor_name = f'executor{i}'
                self.send_task_to_executor(executor_name, part_duration)

            # Отправка отчета информатору после завершения задачи
            report = {'queue': 'informant', 'operation': 'task completed', 'distributor': self.distributor_name}
            self.channel.basic_publish(
                exchange='',
                routing_key='info1',
                body=json.dumps(report)
            )
            print(f"Task completed and reported to informant.")

    def send_task_to_executor(self, executor_name, duration):
        # Заглушка для отправки задачи исполнителю
        print(f"Sending {duration} seconds of task to {executor_name}")
        time.sleep(duration)

    def run_consuming(self):
        self.channel.basic_consume(
            queue=self.distributor_name,
            auto_ack=True,
            on_message_callback=self.callback
        )
        print(f"{self.distributor_name} started listening for tasks.")
        self.channel.start_consuming()

Distributor('distributor1')
