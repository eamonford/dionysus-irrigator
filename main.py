from influxdb import InfluxDBClient
from dao.RuleDataAccessor import RuleDataAccessor
import paho.mqtt.client as mqtt
import Config
import logging

ruleDao = None
sensorDao = None

def on_connect(client, userdata, flags, rc):
    print("Connected to mosquitto with result code "+str(rc))

def on_message(client, userdata, msg):
        messageDict = json.loads(str(msg.payload))
        sensorId = messageDict['sensor_id']
        rules = ruleDao.getBySensorId(sensorId)
        for rule in rules:
            rule['sensor'] = sensorDao.getById(sensorId)

        print rules

def main():
    pgConnection = Config.Configuration().getDatabaseConnection()
    ruleDao = RuleDataAccessor(pgConnection)
    sensorDao = SensorDataAccessor(pgConnection)

    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
