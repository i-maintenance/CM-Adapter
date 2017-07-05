import csv
import datetime as dt
import json
import threading
import logging
import pandas as pd
import requests
from kafka import KafkaProducer

CM_APP_HOST = 'http://192.168.13.101'
KAFKA_TOPIC = 'SensorData'
UPDATE_INTERVAL = 0.1  # in minutes

producer = KafkaProducer(bootstrap_servers=['il061:9092'],
                         api_version=(0, 9),
                         value_serializer=lambda m: json.dumps(m).encode('ascii'))

logger = logging.getLogger(__name__)


def update():
    try:
        sensor_data = fetch_sensor_data()
        publish_sensor_data(sensor_data=sensor_data)
    except Exception as e:
        logger.exception(e)

    # schedule next update
    logger.info('scheduled next update')
    threading.Timer(UPDATE_INTERVAL * 60, update).start()


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


def publish_sensor_data(sensor_data):
    for observation_time in sensor_data.index:
        for sensor in [s for s in sensor_data.columns if s != 'Result']:
            message = {
                'phenomenonTime': str(observation_time),
                'resultTime': str(dt.datetime.now()),
                'result': sensor_data.loc[observation_time, sensor],
                'Datastream': {'@iot.id': 'TODO: REPLACE!!'}}
            producer.send(KAFKA_TOPIC, message)

    logger.info('Published {} sensor entries'.format(len(sensor_data)))

    # block until all messages are sent
    producer.flush()


if __name__ == '__main__':
    update()
