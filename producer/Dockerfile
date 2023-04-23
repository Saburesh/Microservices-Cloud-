FROM python:latest

WORKDIR /producer
COPY . /producer

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "producer.py"]
