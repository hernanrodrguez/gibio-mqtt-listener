class Dispositivo:

    def __init__(self, key, tipo_dispositivo):
        self.key = key
        self.tipo_dispositivo = tipo_dispositivo

    def __init__(self, tup):
        self.id = tup[0]
        self.key = tup[1]
        self.tipo_dispositivo = tup[2]

    def __str__(self):
        return 'Dispositivo(id='+ str(self.id) + ', key=' + self.key + ', tipo_dispositivo=' + str(self.tipo_dispositivo) + ')'
