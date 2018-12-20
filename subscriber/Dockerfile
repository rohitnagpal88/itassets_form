FROM python:3.6

ADD requirements.txt /

RUN mkdir /app

ADD nats_sub.py /app/

RUN pip install -r requirements.txt

WORKDIR /app

ENTRYPOINT ["python","nats_sub.py"]
