from influxdb import InfluxDBClient

def on_connect(client, userdata, flags, rc):
    print("Connected to mosquitto with result code "+str(rc))

def on_message(client, userdata, msg):
        messageDict = json.loads(str(msg.payload))

def main():

    # connect to postgres to get client

    mqttClient = mqtt.Client()
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message
    mqttClient.connect(Config.Configuration().mqttHost, 1883, 60)
    mqttClient.subscribe("dionysus/moisture")

    mqttClient.loop_forever()

main()
