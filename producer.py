import os
import json
from kafka import KafkaProducer
import requests

topic = 'input-data'
data_url = 'http://stream.meetup.com/2/rsvps'
SERVERS = os.environ.get('SERVERS')


def produce():
    producer = KafkaProducer(bootstrap_servers=SERVERS,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    r = requests.get(data_url, stream=True)

    if r.encoding is None:
        r.encoding = 'utf-8'

    for line in r.iter_lines(decode_unicode=True):
        if line:
            producer.send(topic, value=json.loads(line))

    producer.flush()


if __name__ == '__main__':
    produce()
