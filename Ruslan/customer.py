import pika
import json

class AddCustomer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    def __init__(self, new_queue):
        self.task = None # Задача для выполнения
        self.queue = new_queue
        self.informant = None  # Переменная для хранения выбранного информатора
        self.allocator = None  # Переменная для хранения выбранного распределителя
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.get_informant()
        self.start_consuming()


    def callback(self, channel, method, properties, body):
        data = json.loads(body)
        agent = data['queue']
        operation = data['operation']
        message = data['text']
        
        # Обработка ответа от сервера о свободных информаторах
        if agent == 'server' and operation == 'available informant':
            print(f'-- Available informants: {message}')
            self.informant = input('Choose an informant (e.g., informant1): ')
            self.get_allocator(self.informant)
        
        # Обработка ответа от информатора о свободных распределителях
        elif agent.startswith('informant') and operation == 'available allocator':
            print(f'-- Available allocators: {message}')
            self.allocator = input('Choose an allocator to submit a task (e.g., distributor1): ')
            self.submit_a_task(self.allocator)

            # Добавить новую задачу
            self.get_informant()
        
        # Подтверждение завершения задачи от распределителя
        elif agent.startswith('distributor') and operation == 'task completed':
            print('-- Task completed and reported back to customer')

        #  Вывод данных для отладки
        print(f"Processed message from {agent}: {data}")

    def start_consuming(self):
        """Начинает принимать сообщения."""
        self.channel.basic_consume(
            queue=self.queue,
            auto_ack=True,   #Автоматического подтверждения выполненной задачи.
            on_message_callback=self.callback
        )
        self.channel.start_consuming()  # Старт потребления (бесконечный цикл).

    def get_informant(self):
        """Отправляет запрос серверу из консоли для получения информатора."""
        self.task = None  #Обнуление задачи
        text = input('Enter "start"\n')
        if text.lower() == 'start':
            data = {'queue': self.queue, 'operation': 'available informant', 'text': text}
            self.channel.basic_publish(
                exchange='',
                routing_key='server',
                body=json.dumps(data),
                #properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)  # Долговечное сообщение
                )
        else:
            print('Invalid input!')
            self.get_informant()

    def get_allocator(self, informant):
        """Отправляет запрос информатору для получения списка распределителей."""
        data = {'queue': self.queue, 'operation': 'available allocator', 'text': 'request allocators'}
        self.channel.basic_publish(
            exchange='',
            routing_key=informant,
            body=json.dumps(data)
        )
       

    def submit_a_task(self, allocator):
        """Отправляет задачу в распределитель для выполнения"""
        task = input('Enter task duration (in seconds): ')
        if task.isdigit():
            self.task = int(task)
            print('-- Sending task...')
            data = {'queue': self.queue, 'operation': 'submit_task', 'text': self.task}
            self.channel.basic_publish(
                exchange='',
                routing_key=allocator,
                body=json.dumps(data)
            )
            print('-- Task in processing')
        else:
            print("Please enter a valid number for the task!")
            self.submit_a_task(allocator)

# Запуск клиента
AddCustomer('customer1')
