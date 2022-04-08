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
            tipo_medicion = constants.TEMPERATURA_AMBIENTE
            
        if data[0] == constants.KEY_TEMPERATURA_SUJETO:
            tipo_medicion = constants.TEMPERATURA_SUJETO

        if data[0] == constants.KEY_CO2:
            tipo_medicion = constants.CO2

        if data[0] == constants.KEY_SPO2:
            tipo_medicion = constants.SPO2

        if data[0] == constants.KEY_FRECUENCIA_CARDIACA:   
            tipo_medicion = constants.FRECUENCIA_CARDIACA         
        
        meas = medicion.Medicion(valor, fecha, tipo_medicion, key_dispositivo, tipo_dispositivo)
        ret_list.append(meas)

    return ret_list

def db_create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def db_get_query(conn, table, columns="*", clause=""):
    cur = conn.cursor()

    if isinstance(columns, list):
        columns = ", ".join(columns)
    if len(clause) > 1:
        clause = "WHERE {}".format(clause)

    my_str = "SELECT {} FROM {} {}".format(columns, table, clause)
    
    cur.execute(my_str)
    ret = cur.fetchall()
    cur.close()
    return ret

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("#")  # Subscribe to all topics


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    meas_list = data_from_message(msg.topic, str(msg.payload.decode("utf-8"))) 

    conn = db_create_connection("db/checking.sqlite")
    with conn:
        dispos = db_get_query(conn, "dispositivos", "key")
        for dispo in dispos:
            print(dispo)

    '''
    cur = conn.cursor()
    cur.execute("SELECT key FROM dispositivos")
    dispos = cur.fetchall()
    print("[keys]")

    for dispo in dispos:
        print(dispo)

    print('[meas_list]')
    for meas in meas_list:
        print(meas)   

        if (meas.key_dispositivo,) not in dispos:
            query = \'''INSERT INTO dispositivos
                    (key, tipo_dispositivo) 
                    VALUES 
                    (?, ?)\'''
            
            data_tuple = (meas.key_dispositivo, meas.tipo_dispositivo)
            cur.execute(query, data_tuple)
            conn.commit()   

            dispos.append((meas.key_dispositivo,))
        
        query = 'SELECT id FROM dispositivos WHERE key = ?'
        cur.execute(query, (meas.key_dispositivo,))
        id_dispositivo = cur.fetchone()[0]
        
        print('[id_dispositivo]', id_dispositivo)
        
        query = \'''INSERT INTO mediciones
                    (valor, fecha, id_dispositivo, tipo_medicion) 
                    VALUES 
                    (?, ?, ?, ?)\'''
            
        data_tuple = (meas.valor, meas.fecha, id_dispositivo, meas.tipo_medicion)
        cur.execute(query, data_tuple)
        conn.commit()
    
    cur.close()'''
    conn.close()        

client = mqtt.Client("gibio_test_mqtt")  # Create instance of client with client ID gibio_test_mqtt
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
client.connect(constants.MQTT_SERVER_HOSTNAME, constants.MQTT_SERVER_PORT)
client.loop_forever()  # Start networking daemon
'''
db_get_query(None, "dispositivos")
db_get_query(None, "dispositivos", "key")
db_get_query(None, "dispositivos", ["key", "id", "tipo"])

db_get_query(None, "dispositivos")
db_get_query(None, "dispositivos", "key", "id=1")
db_get_query(None, "dispositivos", ["key", "id", "tipo"])'''

