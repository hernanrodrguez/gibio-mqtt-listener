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

def db_get_query(conn, table, columns="*"):
    cur = conn.cursor()

    if isinstance(columns, list):
        columns = ", ".join(columns)

    my_str = "SELECT {} FROM {}".format(columns, table)
    cur.execute(my_str)
    ret = cur.fetchall()
    cur.close()
    return ret

def db_get_query_value(conn, table, columns="*", clause="", value=[]):
    cur = conn.cursor()

    if isinstance(columns, list):
        columns = ", ".join(columns)

    query = "SELECT {} FROM {} WHERE {} ?".format(columns, table, clause)
    print(query)
    print(value)
    cur.execute(query, (value,))
    ret = cur.fetchone()[0]
    cur.close()
    return ret

# meas = db_get_query_values(conn, "mediciones", "valor", ["id_dispositivo", "tipo_medicion"] , [id, value])
def db_get_query_values(conn, table, columns="*", clause="", value=[]):
    cur = conn.cursor()

    if isinstance(columns, list):
        columns = ", ".join(columns)

    query = "SELECT {} FROM {} WHERE {}".format(columns, table, clause)
    print(query)
    cur.execute(query, tuple(value))
    ret = cur.fetchall()
    cur.close()
    return ret

def db_insert_query(conn, table, columns, values):
    cur = conn.cursor()
    question_marks = "?"

    for i in range(len(columns)-1):
        question_marks += ", ?"
    if isinstance(columns, list):
        columns = ", ".join(columns)

    query = "INSERT INTO {} ({}) VALUES ({})".format(table, columns, question_marks)
    cur.execute(query, tuple(values))
    conn.commit()
    cur.close()

def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("#")  # Subscribe to all topics

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    if (msg.topic).split("/")[1] != "request":
        meas_list = data_from_message(msg.topic, str(msg.payload.decode("utf-8")))
        conn = db_create_connection("db/checking.sqlite")
        with conn:
            dispos = db_get_query(conn, "dispositivos", "key")
            for meas in meas_list:
                if (meas.key_dispositivo,) not in dispos:
                    db_insert_query(conn, "dispositivos", ["key", "tipo_dispositivo"], [meas.key_dispositivo, meas.tipo_dispositivo])

                id = db_get_query_value(conn, "dispositivos", "id", "key = ?", meas.key_dispositivo)
                db_insert_query( conn,
                                "mediciones",
                                ["valor", "fecha", "id_dispositivo", "tipo_medicion"],
                                [meas.valor, meas.fecha, id, meas.tipo_medicion]
                               )
                print("Saved -> " + str(meas))
        conn.close()
    else:
        payload = str(msg.payload.decode("utf-8"))
        table = (msg.topic).split("/")[2]
        if table == "dispositivos":
            column =  payload.split("-")[0]
            clause = (payload.split("-")[1]).split(":")[0]
            value = (payload.split("-")[1]).split(":")[1]
            print(payload)
            print(table)
            print(column)
            print(clause)
            print(value)
            conn = db_create_connection("db/checking.sqlite")
            with conn:
                dispos = db_get_query_values(conn, table, column, clause + " = ", value)
                print(dispos)
        else:
            column =  payload.split("-")[0]
            value = (payload.split("-")[1]).split(":")[1]
            conn = db_create_connection("db/checking.sqlite")
            with conn:
                id = db_get_query_value(conn, "dispositivos", "id", "key =", column)
                print(id)
                meas = db_get_query_values(conn, "mediciones", "valor", "id_dispositivo=? AND tipo_medicion=?" , [id, value])
                # tengo que hacer que me concatene las condiciones para leer con muchas a la vez
                # https://www.tutorialspoint.com/sqlite/sqlite_and_or_clauses.htm
                # https://stackoverflow.com/questions/23273242/multiple-where-clauses-in-sqlite3-python
                print(meas)
        # key-tipo_medicion:1
        # db_get_query_value(conn, table, columns="*", clause="", value=[])


client = mqtt.Client("gibio_test_mqtt")  # Create instance of client with client ID gibio_test_mqtt
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
client.connect(constants.MQTT_SERVER_HOSTNAME, constants.MQTT_SERVER_PORT)
client.loop_forever()  # Start networking daemon

# SECCION DE TESTEO
# conn = db_create_connection("db/checking.sqlite")
# db_get_query(None, "dispositivos")
# db_get_query(None, "dispositivos", "key")
# db_get_query(None, "dispositivos", ["key", "id", "tipo"])
#
# db_get_query(None, "dispositivos")
# db_get_query(None, "dispositivos", "key", "id=1")
# db_get_query(None, "dispositivos", ["key", "id", "tipo"])
#
# db_insert_query(conn, "dispositivos", ["key", "tipo_dispositivo"], ["aula_01", 1])
# db_insert_query(conn, "dispositivos", ["key", "tipo_dispositivo"], ["aula_02", 1])
#
# Como hacer para request data: avisar en el topic que vamos a pedir datos para que nos lo devuelva el servidor
# Ejemplo topic (generico): test/request/dipositivos_mediciones
# Ejemplo payload (pido todos los dispositivos habitacion): key-tipo_dispositivo:1
# Ejemplo payload (pido todas las mediciones de XX de una habitacion): aula01-tipo_medicion:1
# Busco el id del dispo a partir de su key. Con el id y el tipo de medicion obtengo las mediciones
# Osea que seria: lo_que_quiero-condicion
