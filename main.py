from dao.RuleDataAccessor import RuleDataAccessor
from dao.SensorDataAccessor import SensorDataAccessor
import json
import paho.mqtt.client as mqtt
import Config
import logging
from spyrk import SparkCloud

IRRIGATOR_MASTER = "dozen_laser"
ACCESS_TOKEN = 'a520cd5bd34112b273fda91b1164f011b81fd2de'

logging.basicConfig()
Logger = logging.getLogger(__name__)
Logger.setLevel(20)

pgConnection = Config.Configuration().getDatabaseConnection()
ruleDao = RuleDataAccessor(pgConnection)
sensorDao = SensorDataAccessor(pgConnection)

def generateIrrigationCommandJson(valveId, secondsToWater):
    return "{\"i\": [{\"id\": " + str(valveId) + ", \"d\": " + str(secondsToWater) + "}]}"

def executeMoistureRule(rule, value, sparkClient):
    if value < rule['threshold']:
        sensor = rule['sensor']
        Logger.info(str(sensor['device_id']) + " NEEDS TO BE WATERED")
        commandJson = generateIrrigationCommandJson(rule['valve_id'], 5)
        print("Sending command: " + commandJson)
        result = sparkClient.devices[IRRIGATOR_MASTER].execute(commandJson)


def on_connect(client, userdata, flags, rc):
    Logger.info("Connected to mosquitto at " + Config.Configuration().mqttHost)

def on_message(mqttClient, sparkClient, msg):
        messageDict = json.loads(str(msg.payload))
        sensorId = messageDict['id']
        rules = ruleDao.getBySensorId(sensorId)
        for rule in rules:
            rule['sensor'] = sensorDao.getById(sensorId)[0]
            if rule['type'] == 'moisture':
                try:
                    executeMoistureRule(rule, messageDict['value'], sparkClient)
                except Exception as e:
                    Logger.exception("Could not contact irrigator " + IRRIGATOR_MASTER + ". The irrigator may be offline.")
    
def main():
    sparkClient = SparkCloud(ACCESS_TOKEN)
    mqttClient = mqtt.Client(userdata = sparkClient)
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
