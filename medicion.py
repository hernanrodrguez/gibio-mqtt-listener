from datetime import datetime

class Medicion:

    fecha = 0
    muestra = 0
    
    def __init__(self, valor=0, fecha_muestra=0, tipo_medicion=0, key_dispositivo='', tipo_dispositivo=0):
        self.valor = valor
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