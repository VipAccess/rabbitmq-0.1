import threading
import pika
import time


class AddCustomer:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  # Подключение.
    channel = connection.channel()  # Создание канала.

    def __init__(self, new_queue):
        self.task = None # Задача для выполнения
        self.queue = new_queue
        self.channel.queue_declare(queue=self.queue, durable=True)  # Новая очередь (долговечная).
        self.channel.basic_qos(prefetch_count=1)  # Количество принимаемых задач.
        self.get_informant()
        self.start_consuming()


    def callback(self, channel, method, properties, body):
        agent, text = body.decode().split('-')
        if agent == 'server':
            print(f'-- Available informant - {text}')
        #     self.get_allocator(text) # Получить распределитель.
        # elif agent.startswith('informant'):
        #     print(f'-- Available allocator - {text}')
        #     self.submit_a_task(text) # Отправить задачу в распределитель.
        #     self.get_informant() # Добавить задачу.


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
            self.channel.basic_publish(
                exchange='',
                routing_key='server',
                body=f'{self.queue}-{text}',
                #properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)  # Долговечное сообщение
                )
        else:
            print('Invalid input!')
            self.get_informant()

    def get_allocator(self, informant):
        """Отправляет запрос информатору для получения распределителя."""
        text = input('Please enter a task(Число):\n')
        self.task = text
        task_information = self.task  # Информация о полученной задаче (Размер данных).
        self.channel.basic_publish(
            exchange='',
            routing_key=informant,
            body=f'{self.queue}-{task_information}',
            #properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)  # Долговечное сообщение
        )

    def submit_a_task(self, allocator):
        """Отправляет задачу в распределитель для выполнения"""
        print('-- Loading...')
        for i in range(5, 0, -1):  # Имитация загрузки
            print(f'{i}...')
        self.channel.basic_publish(
            exchange='',
            routing_key=allocator,
            body=f'{self.queue}-{self.task}',
            # properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)  # Долговечное сообщение
        )
        print('-- Task in processing')




AddCustomer('customer1')