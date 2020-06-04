import paho.mqtt.client as mqtt
import time
import threading
import logging
import os
import json

import HAEmail
import HADetectPerson
import HADatabase
import HAWeather


def on_connect(_mqttc, _obj, _flags, rc):
    """ Declaration of Function when MQTT Client Connects """
    logging.info("Connection to broker RC: %s", str(rc))


def on_publish(_mqttc, _obj, mid):
    """ Declaration of Function when MQTT Client Publish a Msg """

    logging.info("mid: %s", str(mid))


def on_subscribe(_mqttc, _obj, mid, granted_qos):
    """ Declaration of Function when MQTT Client Subscribes a Topic """

    logging.info("Subscribing to topic")
    logging.info("Subscribed: %s %s", str(mid), str(granted_qos))


def on_log(_mqttc, _obj, _level, string):
    """ Declaration of Function when MQTT Client Logs """

    logging.info('%s', string)


def on_message(mqttc, _obj, msg):
    """ Declaration of Function when MQTT Client Gets a Msg """

    logging.info("%s: %s", str(msg.topic), str(msg.payload.decode('utf-8')))

    ####
    # To Do: Standardize way to read messages and actions with json files...
    ####
    topic_array = msg.topic.split('/')
    if len(topic_array) > 2:
        if topic_array[2] == "door":
            logging.info("Door Status Changed")
            if msg.payload.decode('utf-8') == "Opened":
                logging.info("HA-Person-Detection-Started")
                detect_person_t = threading.Thread(
                    target=HADetectPerson.detect_person, args=(mqttc, ))
                detect_person_t.start()
        if topic_array[2] == "ha":
            if msg.payload.decode('utf-8') == "Alive":
                logging.info("Home Assistant is alive")
                alive_email_t = threading.Thread(
                    target=send_statistics, args=())
                alive_email_t.start()
            if msg.payload.decode('utf-8') == "Weather":
                logging.info("Sending Weather")
                alive_email_t = threading.Thread(
                    target=send_weather, args=())
                alive_email_t.start()


def send_statistics():
    """ Temp Function to send HA Statistics """

    logging.info("Sending Statistics")
    try:
        read_db_thread = threading.Thread(target=ha_db.read_database, args=())
        read_db_thread.start()
        logging.info("Waiting for Database to be read")
        read_db_thread.join()
        logging.info("Preparing E-Mail")

        alive_msg = credentials[1]["alive_msg"]
        alive_subject = credentials[1]["alive_subject"]
        alive_from = credentials[1]["alive_from"]
        alive_to = credentials[1]["alive_to"]

        alive_email_thread = threading.Thread(
            target=HAEmail.send_email,
            args=(
                alive_msg,
                alive_subject,
                alive_from,
                alive_to,
            ))
        alive_email_thread.start()

    except Exception as e:  # skipcq: PYL-W0703
        logging.error(e)


def send_weather():
    """ Temp Function to send OW Statistics """

    logging.info("Sending Weather")
    try:
        read_weather_thread = threading.Thread(
            target=ha_weather.get_weather, args=())
        read_weather_thread.start()
        logging.info("Waiting for Weather to be read")
        read_weather_thread.join()

        weather_msg = ha_weather.email_txt
        weather_subject = credentials[2]["weather_subject"]
        weather_from = credentials[2]["weather_from"]
        weather_to = credentials[2]["weather_to"]

        alive_email_thread = threading.Thread(
            target=HAEmail.send_email,
            args=(
                weather_msg,
                weather_subject,
                weather_from,
                weather_to,
            ))
        alive_email_thread.start()

    except Exception as e:  # skipcq: PYL-W0703
        logging.error(e)


if __name__ == '__main__':

    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        filemode='a',
        filename='HA-Watch.log',
        level=logging.INFO,
        datefmt="%H:%M:%S")

    with open('include/credentials.json', 'r') as f:
        credentials = json.load(f)

    # Init Weather
    ha_weather = HAWeather.HAWeather()
    ha_weather.get_weather()

    # Read Database
    path = credentials[0]["db_path"]
    if os.path.isfile(path):
        ha_db = HADatabase.HADatabase(path=path)
        ha_db.read_database()
        ha_db.prepare_email()

    broker_address = credentials[0]["broker_address"]
    port = credentials[0]["port"]
    user = credentials[0]["user"]
    password = credentials[0]["password"]

    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(
        user, password=password)  # set username and password
    mqtt_client.on_message = on_message
    mqtt_client.on_connect = on_connect
    mqtt_client.on_publish = on_publish
    mqtt_client.on_subscribe = on_subscribe
    # mqttc.on_log = on_log # Un/comment to enable debug messages
    mqtt_client.connect(broker_address, port=port)

    mqtt_client.loop_start()  # start the loop
    logging.info("Subscribing to desired topics")
    mqtt_client.subscribe(credentials[0]["all_topics"])

    while 1:
        time.sleep(43200)  # wait
        logging.info("MQTT-HA Watch is alive")

    logging.info("Exiting Loop")
    mqtt_client.loop_stop()  # stop the loop
    # deepcode ignore replace~exit~sys.exit: <please specify a reason of ignoring this>
    exit()
