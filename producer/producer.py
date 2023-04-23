import pika
import time
from flask import Flask, request
import uuid

app = Flask(__name__)
connection = None

# connect to RabbitMQ
def connect_rabbitmq():
    global connection,channel
    parameters = pika.ConnectionParameters(host='172.18.0.2',heartbeat=0)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

# send message to RabbitMQ
def send_message(queue_name, message):
    channel.queue_declare(queue=queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    print(f"Sent message to queue {queue_name}: {message}")
    channel.stop_consuming()

# create health_check endpoint
@app.route('/health_check', methods=['GET'])
def health_check():
    message = "RabbitMQ connection is established."
    send_message("health_check", message)
    # bod=[]
    # def timer(ch, method, properties,body):
    #     print(body)
    #     bod.append(body)
    #     channel.stop_consuming()
    # channel.basic_consume(queue="replyqueue",on_message_callback=timer)
    #channel.start_consuming()
    return "health check done"

# create insert_record endpoint
@app.route('/insert_record/<name>/<srn>/<section>', methods=['GET','POST'])
def insert_record(name,srn,section):
    # name=request.form['name']
    # srn=request.form['srn']
    # section=request.form['section']
    if request.method=="GET":
        message = f"{name},{srn},{section}"
        send_message("insert", message)
        print(f"Sent message to queue insert : {message}")
        return f"Inserted record: {message}"
    # else:
        # return "enter name, srn and section as parameters of the url"

# create read_database endpoint
@app.route('/read_database', methods=['GET'])
def read_database():
    send_message("read", "get all records")
    return "Record read at consumer side"

# create delete_record endpoint
@app.route('/delete_record/<srn>', methods=['GET'])
def delete_record(srn):
    send_message("delete", srn)
    return f"Deleted record with SRN: {srn}"

if __name__ == '__main__':
    connect_rabbitmq()
    app.run(host="0.0.0.0",debug=True)
