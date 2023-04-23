FROM python:latest

WORKDIR /consumer_four
COPY . /consumer_four


RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5005

CMD ["python", "read.py"]