import paho.mqtt.client as mqtt
import datetime
import time
import dispositivo as d
import medicion as m
import constants
import sqlite3
import json

def data_from_message(topic, payload):
    valor = float()
    muestra = int()
    tipo_medicion = int()
    tipo_dispositivo = int()

    fecha = time.mktime(datetime.datetime.now().timetuple())
    fecha = fecha - 3 * 60 * 60 # Resto 3 horas por la zona horaria

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
        # meas = m.Medicion()

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

        meas = m.Medicion(valor, fecha, tipo_medicion, key_dispositivo, tipo_dispositivo)
        ret_list.append(meas)

    return ret_list

def db_create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("#")  # Subscribe to all topics

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    payload = str(msg.payload.decode("utf-8"))
    if msg.topic != "respuesta":
        print("Message received-> " + msg.topic + " " + payload)  # Print a received msg
    if msg.topic.split("/")[0] == constants.SEND: # El equipo envia datos
        meas_list = data_from_message(msg.topic, payload) # Obtengo una lista de mediciones a partir del mensaje
        conn = db_create_connection(constants.DATABASE)
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT key FROM dispositivos") # Me fijo los dispositivos que tengo en mi base de datos
            devices = cur.fetchall()
            for meas in meas_list:
                if (meas.key_dispositivo,) not in devices: # Si el dispositivo es nuevo, lo guardo
                    query = "INSERT INTO dispositivos (key, tipo_dispositivo) VALUES (?, ?)"
                    values = [meas.key_dispositivo, meas.tipo_dispositivo]
                    cur.execute(query, tuple(values))
                    conn.commit()
                query = "SELECT id FROM dispositivos WHERE key = ?"
                cur.execute(query, (meas.key_dispositivo,)) # Me agarro el id del dispositivo al que corresponde la medicion
                id = cur.fetchone()[0]
                query = "INSERT INTO mediciones (valor, fecha, id_dispositivo, tipo_medicion) VALUES (?, ?, ?, ?)" # Inserto la medicion
                values = [meas.valor, meas.fecha, id, meas.tipo_medicion]
                cur.execute(query, tuple(values))
                conn.commit()
                print("Saved -> " + str(meas))
            cur.close()
    elif msg.topic.split("/")[0] == constants.REQUEST:
        conn = db_create_connection(constants.DATABASE)
        with conn:
            cur = conn.cursor()
            table = (msg.topic).split("/")[1] # Tabla que quiere consultar
            if table == constants.DEVICES: # Quiere consultar por los dispositivos
                dispositivos = list()
                tipo_dispositivo = 0
                try:
                    tipo_dispositivo = int(payload)
                except:
                    return
                if tipo_dispositivo == 0: # Quiere todos los dispositivos
                    cur.execute("SELECT * FROM dispositivos")
                elif tipo_dispositivo < 3: # Quiere algun tipo de dispositivos
                    cur.execute("SELECT * FROM dispositivos WHERE tipo_dispositivo = " + str(tipo_dispositivo))
                devices = cur.fetchall()
                for device in devices:
                    dispositivos.append(vars(d.Dispositivo(device))) # Genero una lista con los dispositivos a responder

                print(json.dumps(dispositivos))  # Printeo
                client.publish("respuesta/dispositivos", json.dumps(dispositivos))

                cur.close()
            elif table == constants.MEASUREMENTS:  # Quiere consultar por las mediciones
                mediciones = list()
                measurements = list()
                try:
                    if (msg.topic).split("/")[2] == "last": # Quiere la ultima medicion de cada tipo del dispositivo
                        query = "SELECT * FROM mediciones WHERE " + payload + " AND tipo_medicion=? ORDER BY fecha DESC"
                        # print(query)
                        cur.execute(query, (1,))
                        # print(cur.fetchone())
                        measurements.append(m.Medicion(cur.fetchone()))
                        query = "SELECT * FROM mediciones WHERE " + payload + " AND tipo_medicion=? ORDER BY fecha DESC"
                        # print(query)
                        cur.execute(query, (2,))
                        # print(cur.fetchone())
                        measurements.append(m.Medicion(cur.fetchone()))
                        query = "SELECT * FROM mediciones WHERE " + payload + " AND tipo_medicion=? ORDER BY fecha DESC"
                        # print(query)
                        cur.execute(query, (3,))
                        # print(cur.fetchone())
                        measurements.append(m.Medicion(cur.fetchone()))
                        query = "SELECT * FROM mediciones WHERE " + payload + " AND tipo_medicion=? ORDER BY fecha DESC"
                        # print(query)
                        cur.execute(query, (4,))
                        # print(cur.fetchone())
                        measurements.append(m.Medicion(cur.fetchone()))
                        query = "SELECT * FROM mediciones WHERE " + payload + " AND tipo_medicion=? ORDER BY fecha DESC"
                        # print(query)
                        cur.execute(query, (5,))
                        # print(cur.fetchone())
                        measurements.append(m.Medicion(cur.fetchone()))

                    for measurement in measurements:
                        mediciones.append(vars(measurement)) # Genero una lista con los dispositivos a responder

                    print(json.dumps(mediciones))
                    client.publish("respuesta/mediciones/last", json.dumps(mediciones))
                except:
                    query = "SELECT * FROM mediciones WHERE " + payload + " ORDER BY fecha ASC"
                    cur.execute(query)
                    measurements = cur.fetchall()
                    for measurement in measurements:
                        mediciones.append(vars(m.Medicion(measurement))) # Genero una lista con los dispositivos a responder

                    print(json.dumps(mediciones))  # Printeo
                    client.publish("respuesta/mediciones", json.dumps(mediciones))

                cur.close()


client = mqtt.Client("gibio_test_mqtt")  # Create instance of client with client ID gibio_test_mqtt
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
client.connect(constants.MQTT_SERVER_HOSTNAME, constants.MQTT_SERVER_PORT)
client.loop_forever()  # Start networking daemon
