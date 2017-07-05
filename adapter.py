import csv
from datetime import datetime, timedelta
import json
import threading
import logging
import pandas as pd
import requests
from kafka import KafkaProducer

CM_APP_HOST = 'http://192.168.13.101'
KAFKA_TOPIC = 'SensorData'
BOOTSTRAP_SERVERS = ['il061:9092']
UPDATE_INTERVAL = 2  # in minutes

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                         api_version=(0, 9),
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def update(last_sent_time=None):
    try:
        sensor_data = fetch_sensor_data()
        logger.info('Fetched {} sensor entries'.format(len(sensor_data)))
        last_sent_time, num_entries_sent = publish_sensor_data(sensor_data=sensor_data,
                                                               last_sent_time=last_sent_time)
        logger.info('Published {} new sensor entries till {}'.format(num_entries_sent, last_sent_time))
    except Exception as e:
        logger.exception(e)

    # schedule next update
    next_time = datetime.now() + timedelta(minutes=UPDATE_INTERVAL)
    threading.Timer((next_time - datetime.now()).total_seconds(),
                    function=update,
                    kwargs={'last_sent_time': last_sent_time}).start()
    logger.info('Scheduled next update at {}'.format(next_time))


def fetch_sensor_data():
    url = CM_APP_HOST + '/FileBrowser/Download?Path=/DataLogs/SalzburgResearch_Logging.csv'
    headers = {'Referer': 'http://192.168.13.101/Portal/Portal.mwsl?PriNav=FileBrowser&Path=/DataLogs/"'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    csv_data = response.text.splitlines()

    # read sensor data from csv file
    csv_reader = csv.reader(csv_data)
    csv_header = next(csv_reader)

    sensor_data = pd.DataFrame(columns=csv_header)
    for row in csv_reader:
        if row:  # due to blank line at the bottom
            sensor_data.loc[row[0]] = list(row)

    # convert timestamp
    sensor_data['Zeitstempel'] = pd.to_datetime(sensor_data['Zeitstempel'])
    sensor_data = sensor_data.set_index(['Zeitstempel']).sort_index()

    return sensor_data


def publish_sensor_data(sensor_data, last_sent_time=None):
    if not last_sent_time:
        last_sent_time = datetime.fromtimestamp(0)

    data_to_send = sensor_data.ix[sensor_data.index > last_sent_time]
    for observation_time in data_to_send.index:
        for sensor in [s for s in data_to_send.columns if s != 'Result']:
            message = {
                'phenomenonTime': str(observation_time),
                'resultTime': str(datetime.now()),
                'result': sensor_data.loc[observation_time, sensor],
                'Datastream': {'@iot.id': sensor}}
            producer.send(KAFKA_TOPIC, message)

    # block until all messages are sent
    producer.flush()

    # return last sent time
    return sensor_data.index[-1], len(data_to_send)


if __name__ == '__main__':
    update()
