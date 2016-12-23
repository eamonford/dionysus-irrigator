from dao.RuleDataAccessor import RuleDataAccessor
from dao.SensorDataAccessor import SensorDataAccessor
import json
import paho.mqtt.client as mqtt
import Config
import logging

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)
sensorDao = SensorDataAccessor(pgConnection)

def executeMoistureRule(rule, value):
    if value < rule['threshold']:
        sensor = rule['sensor']
        Logger.info(str(sensor['device_id']) + " NEEDS TO BE WATERED")

def on_connect(client, userdata, flags, rc):
    Logger.info("Connected to mosquitto at " + Config.Configuration().mqttHost)

def on_message(client, userdata, msg):
        messageDict = json.loads(str(msg.payload))
        sensorId = messageDict['id']
        rules = ruleDao.getBySensorId(sensorId)
        for rule in rules:
            rule['sensor'] = sensorDao.getById(sensorId)[0]
            if rule['type'] == 'moisture':
                executeMoistureRule(rule, messageDict['value'])

def main():
    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
