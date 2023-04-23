FROM python:latest

WORKDIR /consumer_one

COPY . /consumer_one

RUN pip install -r requirements.txt

EXPOSE 5002

CMD ["python", "healthcheck.py"]

