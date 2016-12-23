FROM resin/rpi-raspbian

RUN apt-get update && \
    apt-get install -y python-pip && \
    apt-get install postgresql && \
    apt-get install python-psycopg2 && \
    apt-get install libpq-dev && \
    pip install paho-mqtt

RUN mkdir /irrigator
COPY . /irrigator
CMD ["python", "-u", "/irrigator/main.py"]
