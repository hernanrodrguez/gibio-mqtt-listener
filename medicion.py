from datetime import datetime

class Medicion:

    fecha = 0
    muestra = 0
    
    def __init__(self, valor=0, fecha_muestra=0, id_dispositivo=0, tipo_medicion=0):
        self.valor = valor
        self.id_dispositivo = id_dispositivo
        self.tipo_medicion = tipo_medicion
        #if isinstance(fecha_muestra, datetime.datetime):
        if isinstance(fecha_muestra, float):
            self.fecha = fecha_muestra
        elif isinstance(fecha_muestra, int):
            self.muestra = fecha_muestra
        else:
            raise TypeError("No se recibio ni fecha ni número de muestra")
            
    def __str__(self):
        return 'Medicion(valor=' + str(self.valor) + ', id_dispositivo=' + str(self.id_dispositivo) + ', tipo_medicion=' + str(self.tipo_medicion) + ', fecha=' +  datetime.utcfromtimestamp(self.fecha).strftime('%d/%m/%Y-%H:%M:%S') + ', muestra=' + str(self.muestra) + ')'