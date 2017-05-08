from dao.RuleDataAccessor import RuleDataAccessor
from dao.SensorDataAccessor import SensorDataAccessor
import json
import paho.mqtt.client as mqtt
import Config
import logging

ACCESS_TOKEN = 'a520cd5bd34112b273fda91b1164f011b81fd2de'
SECONDS_TO_WATER = 5

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)
sensorDao = SensorDataAccessor(pgConnection)

irrigatorMaster = Config.Configuration().irrigatorMaster
topic = "dionysus/" + irrigatorMaster

def generateIrrigationCommandJson(valveId, secondsToWater):
    return "{\"i\": [{\"id\": " + str(valveId) + ", \"d\": " + str(secondsToWater) + "}]}"

def executeMoistureRule(rule, value, mqttClient):
    if value < rule['threshold']:
        sensor = rule['sensor']
        Logger.info(str("Will attempt to irrigate " + sensor['device_id']))
        commandJson = generateIrrigationCommandJson(rule['valve_id'], SECONDS_TO_WATER)
        mqttClient.publish(topic, commandJson)
        Logger.info(str("Sent irrigation command to MQTT topic " + topic))


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
                    Logger.exception("There was a problem sending the irrigation command to MQTT topic " + topic)

def main():
    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
