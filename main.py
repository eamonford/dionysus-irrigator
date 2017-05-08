from dao.RuleDataAccessor import RuleDataAccessor
from dao.SensorDataAccessor import SensorDataAccessor
import json
import paho.mqtt.client as mqtt
import Config
import logging

IRRIGATOR_MASTER = "dozen_laser"
ACCESS_TOKEN = 'a520cd5bd34112b273fda91b1164f011b81fd2de'
SECONDS_TO_WATER = 5

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)
sensorDao = SensorDataAccessor(pgConnection)

def generateIrrigationCommandJson(valveId, secondsToWater):
    return "{\"i\": [{\"id\": " + str(valveId) + ", \"d\": " + str(secondsToWater) + "}]}"

def executeMoistureRule(rule, value, mqttClient):
    if value < rule['threshold']:
        sensor = rule['sensor']
        Logger.info(str("Will attempt to irrigate " + sensor['device_id']))
        commandJson = generateIrrigationCommandJson(rule['valve_id'], SECONDS_TO_WATER)
        mqttClient.publish("dionysus/" + IRRIGATOR_MASTER, commandJson)
        Logger.info(str("Sent irrigation command to " + IRRIGATOR_MASTER))


def on_connect(client, userdata, flags, rc):
    Logger.info("Connected to mosquitto at " + Config.Configuration().mqttHost)

def on_message(mqttClient, userdata, msg):
        messageDict = json.loads(str(msg.payload))
        sensorId = messageDict['id']
        rules = ruleDao.getBySensorId(sensorId)
        for rule in rules:
            rule['sensor'] = sensorDao.getById(sensorId)[0]
            if rule['type'] == 'moisture':
                try:
                    executeMoistureRule(rule, messageDict['value'], mqttClient)
                except Exception as e:
                    Logger.exception("Could not contact irrigator " + IRRIGATOR_MASTER + ". The irrigator may be offline.")

def main():
    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
