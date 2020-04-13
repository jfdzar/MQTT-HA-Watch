import paho.mqtt.client as mqtt
import time
import threading
import logging
import os
import json

import HAEmail
import HADetectPerson
import HADatabase

def on_connect(_mqttc, _obj, _flags, rc):
    logging.info("Connection to broker RC: %s", str(rc))

def on_publish(_mqttc, _obj, mid):
    logging.info("mid: %s",str(mid))

def on_subscribe(_mqttc, _obj, mid, granted_qos):
    logging.info("Subscribing to topic")
    logging.info("Subscribed: %s %s",str(mid),str(granted_qos))

def on_log(_mqttc, _obj, _level, string):
    logging.info('%s',string)

def on_message(mqttc, _obj, msg):
    logging.info("%s: %s",str(msg.topic),str(msg.payload.decode('utf-8')))
    
    ####
    # To Do: Standardize way to read messages and actions with json files...
    ####
    topic_array = msg.topic.split('/')
    if len(topic_array)>2:
        if topic_array[2] == "door":
            logging.info("Door Status Changed")
            if msg.payload.decode('utf-8') == "Opened":
                logging.info("HA-Person-Detection-Started")
                detect_person_thread = threading.Thread(target=HADetectPerson.detect_person, args=(mqttc,))
                detect_person_thread.start()
        if topic_array[2] == "ha":
            if msg.payload.decode('utf-8') == "Alive":
                logging.info("Home Assistant is alive")
                alive_email_thread = threading.Thread(target=send_statistics, args=())
                alive_email_thread.start()
                
def send_statistics():
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

        alive_email_thread = threading.Thread(target=HAEmail.send_email, args=(alive_msg,alive_subject,alive_from,alive_to,))
        alive_email_thread.start()
        
    except Exception as e:
        logging.error(e)


if __name__ == '__main__':

    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    with open('include/credentials.json', 'r') as f:
        credentials = json.load(f)


    # Read Database 
    path=credentials[0]["db_path"]
    if os.path.isfile(path):
        ha_db = HADatabase.HADatabase(path=path)
        ha_db.read_database()
        ha_db.prepare_email()

    broker_address= credentials[0]["broker_address"]
    port = credentials[0]["port"]
    user = credentials[0]["user"]
    password = credentials[0]["password"]

    mqttc = mqtt.Client()
    mqttc.username_pw_set(user, password=password)    #set username and password
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    #mqttc.on_log = on_log # Un/comment to enable debug messages
    mqttc.connect(broker_address, port=port)

    mqttc.loop_start() #start the loop
    logging.info("Subscribing to desired topics")
    mqttc.subscribe(credentials[0]["all_topics"])

    while(1):
        time.sleep(43200) # wait
        logging.info("MQTT-HA Watch is alive") 

    logging.info("Exiting Loop") 
    mqttc.loop_stop() #stop the loop
    exit()
