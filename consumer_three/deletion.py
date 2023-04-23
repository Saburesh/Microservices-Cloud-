# import pika
# import mysql.connector

# # RabbitMQ connection setup
# rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', 5672))
# channel = rabbitmq_connection.channel()

# # MySQL connection setup
# mysql_connection = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="password",
#   database="mydatabase"
# )

# # RabbitMQ queue setup
# channel.queue_declare(queue='delete_record', durable=True)

# # MySQL cursor
# cursor = mysql_connection.cursor()

# # RabbitMQ callback function
# def delete_record_callback(ch, method, properties, body):
#     srn = body.decode('utf-8')
#     sql = "DELETE FROM students WHERE SRN = %s"
#     val = (srn,)
#     cursor.execute(sql, val)
#     mysql_connection.commit()
#     print("Deleted record with SRN: " + srn)
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# # RabbitMQ consume from delete_record queue
# channel.basic_qos(prefetch_count=1)
# channel.basic_consume(queue='delete_record', on_message_callback=delete_record_callback)

# # Start consuming
# print(' [*] Waiting for messages. To exit press CTRL+C')
# channel.start_consuming()
import pika
import mysql.connector

def delete_record(ch, method, properties, body):
    try:
        srn = body.decode()
        cnx = mysql.connector.connect(user='root', password='k', host='mymariadb', database='student_record')
        cursor = cnx.cursor()
        query = f"DELETE FROM student WHERE srn='{srn}'"
        cursor.execute(query)
        cnx.commit()
        print(f"Deleted record for SRN {srn}")
        cursor.close()
        cnx.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error while deleting record for SRN {srn}: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='delete')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='delete', on_message_callback=delete_record)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
