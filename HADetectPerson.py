import paho.mqtt.client as mqtt
import subprocess
import logging
import json

def detect_person(mqttc):

    with open('include/person.json', 'r') as f:
        person = json.load(f)

    for item in person:
        address = item['address']
        res = subprocess.call(['ping', '-c', '3', address]) 
        if res == 0: 
            logging.info("Ping to %s is OK: %s is at Home!", address,item['person_name'])
            mqttc.publish(topic=item['person_topic'],payload="Home")
        elif res == 2: 
            logging.info("no response from %s", address) 
        else: 
            #print("ping to", address, "failed!")
            logging.info("Ping to %s is FAILED: %s is Away!", address,item['person_name'])
            mqttc.publish(topic=item['person_topic'],payload="Away")

if __name__ == '__main__':
    
    logging.basicConfig(format='%(asctime)s: %(message)', level=logging.INFO,
                        datefmt="%H:%M:%S")

    with open('include/credentials.json', 'r') as f:
        credentials = json.load(f)

    broker_address= credentials[0]["broker_address"]
    port = credentials[0]["port"]
    user = credentials[0]["user"]
    password = credentials[0]["password"]

    mqttc = mqtt.Client()
    mqttc.username_pw_set(user, password=password)    #set username and password
    mqttc.connect(broker_address, port=port)

    detect_person(mqttc)

    # deepcode ignore replace~exit~sys.exit: <please specify a reason of ignoring this>
    exit()