import pika
import mysql.connector
import os

# MySQL database configuration
mysql_config = {
  'user': 'root',
  'password': 'k',
  'host': 'mymariadb',
  'database': 'student_record'
}

# RabbitMQ configuration
# rabbitmq_config = {
#   'host': 'localhost',
#   'queue_name': 'insert_record',
#   'routing_key': 'insert_record',
# }

def on_message_received(channel, method, properties, body):
    # Extract data from message body
    data = body.decode().split(',')

    # Insert data into MySQL database
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    sql = "INSERT INTO student (name, srn, section) VALUES (%s, %s, %s)"
    values = (data[0], data[1], data[2])
    cursor.execute(sql, values)
    connection.commit()
    cursor.close()
    channel.basic_ack(delivery_tag=method.delivery_tag)
    connection.close()

    # Acknowledge message
    #channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    # Connect to RabbitMQ and start consuming messages
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    #channel.exchange_declare(exchange=rabbitmq_config['exchange_name'], exchange_type='direct')
    channel.queue_declare(queue="insert")
    #channel.queue_bind(exchange=rabbitmq_config['exchange_name'], queue=rabbitmq_config['queue_name'], routing_key=rabbitmq_config['routing_key'])
    #channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="insert", on_message_callback=on_message_received)
    channel.start_consuming()

if __name__ == '__main__':
    main()
