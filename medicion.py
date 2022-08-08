from datetime import datetime

class Medicion:

    fecha = 0
    muestra = 0

    def __init__(self, tup_value, fecha_muestra=0, tipo_medicion=0, key_dispositivo='', tipo_dispositivo=0):
        if isinstance(tup_value, tuple):
            self.valor = tup_value[1]
            self.tipo_medicion = tup_value[5]
            self.id_dispositivo = tup_value[4]
            self.fecha = tup_value[2]
            self.muestra = tup_value[3]
            self.key_dispositivo = ""
            self.tipo_dispositivo = None
        else:
            self.valor = tup_value
            self.key_dispositivo = key_dispositivo
            self.tipo_medicion = tipo_medicion
            self.tipo_dispositivo = tipo_dispositivo
            if isinstance(fecha_muestra, float):
                self.fecha = fecha_muestra
            elif isinstance(fecha_muestra, int):
                self.muestra = fecha_muestra
            else:
                raise TypeError("No se recibio ni fecha ni número de muestra")


    def __str__(self):
        return 'Medicion(valor=' + str(self.valor) + ', tipo_medicion=' + str(self.tipo_medicion) + ', fecha=' +  datetime.utcfromtimestamp(self.fecha).strftime('%d/%m/%Y-%H:%M:%S') + ', muestra=' + str(self.muestra) + ', key_dispositivo=' + self.key_dispositivo + ', tipo_dispositivo=' + str(self.tipo_dispositivo) + ')'
