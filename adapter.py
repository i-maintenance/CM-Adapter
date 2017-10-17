import os
import csv
import json
import logging
import threading
from datetime import datetime, timedelta
import sys

import requests
import pandas as pd
from kafka import KafkaProducer
from logstash import TCPLogstashHandler

CM_APP_HOST = 'http://192.168.13.101'
KAFKA_TOPIC = 'SensorData'
BOOTSTRAP_SERVERS = ['il061:9092', 'il062:9092', 'il063:9092']
IGNORED_FIELDS = ['Record', 'Wind', 'Temp_Aussen', 'Feuchte_Aussen']
UPDATE_INTERVAL = 1  # in minutes

LOGSTASH_HOST = os.getenv('LOGSTASH_HOST', 'localhost')
LOGSTASH_PORT = int(os.getenv('LOGSTASH_PORT', '5000'))

SENSORTHINGS_HOST = os.getenv('SENSORTHINGS_HOST', 'il060')
SENSORTHINGS_PORT = os.getenv('SENSORTHINGS_PORT', '8082')

# setup logging
logger = logging.getLogger('cm-adapter')
logger.setLevel(logging.INFO)
console_logger = logging.StreamHandler(stream=sys.stdout)
console_logger.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
logstash_handler = TCPLogstashHandler(host=LOGSTASH_HOST, port=LOGSTASH_PORT, version=1)
[logger.addHandler(l) for l in [console_logger, logstash_handler]]
logger.info('Sending logstash to %s:%d', logstash_handler.host, logstash_handler.port)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         api_version=(0, 9),
                         value_serializer=lambda m: json.dumps(m).encode('ascii'),
                         acks='all')


def update(last_sent_time=None, id_map=None):
    """
    Fetches recent sensor data from the CM setup and forwards new entries to the i-Maintenance messaging bus.
    After fetching and updating data a new cycle is scheduled.
    :param last_sent_time: Last time of previous update. Used to determine new entries.
        If `None`, all entries will be forwarded.
    :param id_map: Given mapping of SensorThings IDs. This map is updated during the iteration.
    """
    try:
        # fetch sensor data
        sensor_data = fetch_sensor_data(cm_host=CM_APP_HOST)
        logger.info('Fetched {} sensor entries'.format(len(sensor_data)))

        # filter data
        sensor_data = sensor_data.ix[sensor_data.index > last_sent_time] if last_sent_time else sensor_data

        # fetch id mapping
        id_map = fetch_id_mapping(host=SENSORTHINGS_HOST, port=SENSORTHINGS_PORT, fallback=id_map)

        # delegate to messaging bus
        publish_sensor_data(data=sensor_data, id_map=id_map, topic=KAFKA_TOPIC, ignored=IGNORED_FIELDS)
        last_sent_time = sensor_data.index[-1]
        logger.info('Published {} new sensor entries till {}'.format(len(sensor_data), last_sent_time))

    except Exception as e:
        logger.exception(e)

    # schedule next update
    interval = timedelta(minutes=UPDATE_INTERVAL).total_seconds()
    kwargs = {'last_sent_time': last_sent_time, 'id_map': id_map}
    threading.Timer(interval=interval, function=update, kwargs=kwargs).start()
    logger.info('Scheduled next update at {}'.format(datetime.now() + timedelta(minutes=UPDATE_INTERVAL)))


def fetch_sensor_data(cm_host):
    """
    Fetches sensor data from the CM host.
    :param cm_host: URL of CH host.
    :return: Dataframe containing fetched entries, whereas the column matches the columns in the CSV file.
    """
    url = cm_host + '/FileBrowser/Download?Path=/DataLogs/SalzburgResearch_Logging.csv'
    headers = {'Referer': cm_host + '/Portal/Portal.mwsl?PriNav=FileBrowser&Path=/DataLogs/"'}
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


def publish_sensor_data(data, id_map, topic, ignored=None):
    """
    Published sensor data to the i-Maintenance messaging bus (Kafka) using the SensorThings format.
    :param data: Dataframe containing sensor data.
    :param id_map: `dict` of mapping columns in the Dataframe to IDs in the SensorThings domain.
    :param topic: Kafka topic in which the messages are published.
    :param ignored: List of ignored column names. If `None` rows of all columns are published.
    """
    if ignored is None:
        ignored = []

    for observation_time in data.index:
        for sensor in [s for s in data.columns if s not in ignored]:
            message = {'phenomenonTime': observation_time.isoformat(),
                       'resultTime': datetime.now().isoformat(),
                       'result': float(data.loc[observation_time, sensor]),
                       'Datastream': {'@iot.id': id_map[sensor]}}
            producer.send(topic, message)

    # block until all messages are sent
    producer.flush()


def fetch_id_mapping(host, port, fallback):
    """
    Fetches IDs from SensorThings server and creates a dictionary with the proper ID mapping.
    :param host: Host of SensorThings server.
    :param port: Port of SensorThings server.
    :param fallback: Fallback mapping in case of an error. If `None` the actual error while be raised.
    :return: `dict`, which is mapping CM specific IDs to global SensorThings IDs.
    """
    mapping = dict()
    try:
        url = 'http://{}:{}/v1.0/Datastreams'.format(host, port)
        while True:
            url = url.replace('localhost', host).replace('8080', port)  # replace wrong base url and port
            datastreams = requests.get(url=url).json()

            mapping.update({d['name']: d['@iot.id'] for d in datastreams['value']})

            if '@iot.nextLink' not in datastreams:
                break
            url = datastreams['@iot.nextLink']

        logger.info('Fetched id mapping: %s', mapping, extra={'': mapping})
    except Exception as e:
        if not fallback:
            raise e
        logger.warning('Could not fetch id mapping...')
        return fallback

    return mapping


if __name__ == '__main__':
    update()
