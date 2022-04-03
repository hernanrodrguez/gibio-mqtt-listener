import datetime

class Medicion:
    
    def __init__(self, valor, fecha_muestra, id_dispositivo, tipo_medicion):
        self.valor = valor
        self.id_dispositivo = id_dispositivo
        self.tipo_medicion = tipo_medicion
        if isinstance(fecha_muestra, datetime):
            self.fecha = fecha_muestra
        elif isinstance(fecha_muestra, int):
            self.muestra = fecha_muestra
        else:
            raise TypeError("No se recibio ni fecha ni número de muestra")