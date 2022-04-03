import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("#")  # Subscribe to all topics


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    
    data_list = str(msg.payload.decode("utf-8")).split('-')
    for data_item in data_list:
        data = data_item.split(':')
        if data[0] == 'TA':
            print('Temperatura Ambiente:', data[1])
        if data[0] == 'TO':
            print('Temperatura Objeto:', data[1])
        if data[0] == 'C':
            print('CO2:', data[1])
        if data[0] == 'S':
            print('SPO2:', data[1])
        if data[0] == 'HR':
            print('Heart Rate:', data[1])    


client = mqtt.Client("digi_mqtt_test")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.connect('127.0.0.1', 1883)
client.loop_forever()  # Start networking daemon

