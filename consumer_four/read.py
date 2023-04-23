import pika
import mysql.connector
import json

def read_database(ch, method, properties, body):
    try:
        cnx = mysql.connector.connect(user='root', password='k', host='mymariadb', database='student_record')
        cursor = cnx.cursor()
        query = f"SELECT * FROM student"
        cursor.execute(query)
        records = cursor.fetchall()
        print(records)
        # for record in records:
        #     print(record)
        cursor.close()
        cnx.close()
        # ch.basic_publish('',routing_key=properties.reply_to,body=json.dumps(records))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error while reading database: {e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()
channel.queue_declare(queue='read')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='read', on_message_callback=read_database)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
