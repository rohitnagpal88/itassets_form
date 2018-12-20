FROM python:3.6

ADD requirements.txt /

RUN mkdir /app

ADD static /app/
ADD templates /app/
ADD flaskform.py /app/

RUN pip install -r requirements.txt

WORKDIR /app

EXPOSE 5000

ENTRYPOINT ["python","flaskform.py"]
