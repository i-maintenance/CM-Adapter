import os
import csv
import json
import logging
import threading
from datetime import datetime, timedelta
import sys

import pytz
import requests
import pandas as pd
from kafka import KafkaProducer
from logstash import TCPLogstashHandler

CM_APP_HOST = 'http://192.168.13.101'
KAFKA_TOPIC = 'SensorData'
BOOTSTRAP_SERVERS = ['il061:9092', 'il062:9092', 'il063:9092']
UPDATE_INTERVAL = 1  # in minutes

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_logger = logging.StreamHandler(stream=sys.stdout)
console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logstash_handler = TCPLogstashHandler(host=os.getenv('LOGSTASH_HOST', 'localhost'),
                                      port=int(os.getenv('LOGSTASH_PORT', 5000)),
                                      version=1)
[logger.addHandler(l) for l in [console_logger, logstash_handler]]
logger.info('Sending logstash to %s:%d', logstash_handler.host, logstash_handler.port)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         api_version=(0, 9),
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))


def update(last_sent_time=None):
    try:
        # fetch sensor data
        sensor_data = fetch_sensor_data()
        logger.info('Fetched {} sensor entries'.format(len(sensor_data)))

        # filter data
        data_to_send = sensor_data.ix[sensor_data.index > last_sent_time] if last_sent_time else sensor_data

        # delegate to messaging bus
        publish_sensor_data(sensor_data=data_to_send)
        last_sent_time = sensor_data.index[-1]
        logger.info('Published {} new sensor entries till {}'.format(len(data_to_send), last_sent_time))

    except Exception as e:
        logger.exception(e)

    # schedule next update
    interval = timedelta(minutes=UPDATE_INTERVAL).total_seconds()
    threading.Timer(interval=interval, function=update, kwargs={'last_sent_time': last_sent_time}).start()
    logger.info('Scheduled next update at {}'.format(datetime.now() + timedelta(minutes=UPDATE_INTERVAL)))


def fetch_sensor_data():
    url = CM_APP_HOST + '/FileBrowser/Download?Path=/DataLogs/SalzburgResearch_Logging.csv'
    headers = {'Referer': CM_APP_HOST + '/Portal/Portal.mwsl?PriNav=FileBrowser&Path=/DataLogs/"'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    csv_data = response.text.splitlines()

    # read sensor data from csv file
    csv_reader = csv.reader(csv_data)
    csv_header = next(csv_reader)  # read header information

    sensor_data = pd.DataFrame(columns=csv_header)
    for row in csv_reader:
        if row:  # due to blank line at the bottom
            sensor_data.loc[row[0]] = list(row)  # index is first field (i.e. 'Record')

    # convert timestamp
    sensor_data['Zeitstempel'] = pd.to_datetime(sensor_data['Zeitstempel'])
    sensor_data = sensor_data.set_index(['Zeitstempel']).sort_index()

    return sensor_data


def publish_sensor_data(sensor_data):
    for observation_time in sensor_data.index:
        for sensor in [s for s in sensor_data.columns if s != 'Record']:
            message = {'phenomenonTime': observation_time.replace(tzinfo=pytz.UTC).isoformat(),
                       'resultTime': datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat(),
                       'result': float(sensor_data.loc[observation_time, sensor]),
                       'Datastream': {'@iot.id': sensor}}
            producer.send(KAFKA_TOPIC, message)

    # block until all messages are sent
    producer.flush()


if __name__ == '__main__':
    update()
