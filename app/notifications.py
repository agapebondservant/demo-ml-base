import pika
import os
from dotenv import load_dotenv

load_dotenv()


def send_argo_event(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv('rmq_host'),
        credentials=pika.PlainCredentials(os.getenv('rmq_user'), os.getenv('rmq_password'))))
    channel = connection.channel()
    channel.basic_publish(exchange=os.getenv('rmq_exchange'),
                          routing_key=os.getenv('rmq_routing_key'),
                          body=message)
