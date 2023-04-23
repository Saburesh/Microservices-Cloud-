# import pika
# import requests

# # RabbitMQ connection setup
# connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq_network'))
# channel = connection.channel()

# # Declare the queue and binding
# queue_name = 'health_check'
# channel.queue_declare(queue=queue_name)
# channel.queue_bind(queue=queue_name, exchange='amq.direct', routing_key=queue_name)

# # Flask app setup
# from flask import Flask

# app = Flask(__name__)

# # Flask endpoint to handle health check requests
# @app.route('/health_check', methods=['GET'])
# def health_check():
#     # Process the health check request here
#     print("Received health check request")
#     # Acknowledge the message has been received and processed
#     channel.basic_ack(method_frame.delivery_tag)
#     return "OK"

# # Start the Flask app
# if __name__ == '__main__':
#     app.run(host='localhost', port=8080)
import pika
import json

QUEUE_NAME = 'health_check'

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq",heartbeat=0))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=QUEUE_NAME)

# Define the callback function for incoming messages
def callback(ch, method, properties, body):
    # Process the incoming message
    print(f"Received message: {body.decode()}")
    # ch.basic_publish('',routing_key=properties.reply_to,body=json.dumps("health is fine"))
    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming the queue messages
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

# Keep the consumer running
print(f"Consumer started. Waiting for messages on queue '{QUEUE_NAME}'...")
channel.start_consuming()



