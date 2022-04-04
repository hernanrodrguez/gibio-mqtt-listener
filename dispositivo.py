class Dispositivo:
    
    def __init__(self, key, tipo_dispositivo):
        self.key = key
        self.tipo_dispositivo = tipo_dispositivo
        
    def __str__(self):
        return 'Dispositivo(key=' + self.key + ', tipo_dispositivo=' + self.tipo_dispositivo + ')'