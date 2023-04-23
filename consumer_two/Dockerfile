FROM python:latest

WORKDIR /consumer_two
COPY . ./consumer_two

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5003

CMD ["python", "insertion.py"]
