FROM python:latest

WORKDIR /consumer_three
COPY . /consumer_three


RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5004


CMD ["python", "deletion.py"]
