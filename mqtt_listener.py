import paho.mqtt.client as mqtt
import datetime
import time
import dispositivo
import medicion
import constants
import sqlite3

def data_from_message(topic, payload):
    valor = float()
    muestra = int()
    tipo_medicion = int()
    tipo_dispositivo = int()
    
    fecha = time.mktime(datetime.datetime.now().timetuple())
    fecha = fecha - 3 * 60 * 60 # Resto 3 horas por la zona horaria
    
    print('[data_from_message] topic:' + topic + ' - payload:' + payload)
    topic = topic.split('/') 
    
    key_dispositivo = topic[2]
    
    if topic[1] == constants.HABITACION:
        tipo_dispositivo = constants.DISPO_HABITACION
    elif topic[1] == constants.PERSONA:
        tipo_dispositivo = constants.DISPO_PERSONA
    else:
        return list()
        
    ret_list = list()
    data_list = payload.split('-')    
    
    for data_item in data_list:
        meas = medicion.Medicion()
        
        data = data_item.split(':')         
        valor = float(data[1])        
               
        if data[0] == constants.KEY_TEMPERATURA_AMBIENTE:
            #print('Temperatura Ambiente:', data[1])            
            tipo_medicion = constants.TEMPERATURA_AMBIENTE
            
        if data[0] == constants.KEY_TEMPERATURA_SUJETO:
            #print('Temperatura Sujeto:', data[1])
            tipo_medicion = constants.TEMPERATURA_SUJETO

        if data[0] == constants.KEY_CO2:
            #print('CO2:', data[1])
            tipo_medicion = constants.CO2

        if data[0] == constants.KEY_SPO2:
            #print('SPO2:', data[1])
            tipo_medicion = constants.SPO2

        if data[0] == constants.KEY_FRECUENCIA_CARDIACA:
            #print('Frecuencia cardiaca:', data[1])    
            tipo_medicion = constants.FRECUENCIA_CARDIACA         
        
        meas = medicion.Medicion(valor, fecha, key_dispositivo, tipo_medicion)
        ret_list.append(meas)
        #print(meas)

    return ret_list

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("#")  # Subscribe to all topics


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    meas_list = data_from_message(msg.topic, str(msg.payload.decode("utf-8"))) 
    print('[meas_list]')
    for meas in meas_list:
        print(meas)
    

client = mqtt.Client("gibio_test_mqtt")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
client.connect(constants.MQTT_SERVER_HOSTNAME, constants.MQTT_SERVER_PORT)
client.loop_forever()  # Start networking daemon

