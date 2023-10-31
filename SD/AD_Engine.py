#-Conexión con weather
#-conexión con el fichero de dibujo y obtención num de drones
#-comprobar num drones en registro.txt
#-enviar cada pos requerida a cada dron
#-expresion del mapa a cada mov de dron


import socket
import sys
import threading
import time
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pickle


HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
SERVER = socket.gethostbyname(socket.gethostname())
MAX_CONEXIONES = 10

# Códigos de escape ANSI para colores de fondo
FONDO_ROJO = "\033[41m"
FONDO_VERDE = "\033[42m"
FONDO_CREMA = "\033[48;5;224m"
RESET = "\033[0m"
TEXTO_ROJO = "\033[31m"
TEXTO_NEGRO = "\033[30m"
LETRA_GROSOR_NEGRITA = "\033[1m"

cuadrado = "□"

temp =0
output_lock = threading.Lock()

##########   MAPA   ##############

def imprimir_mapa_actualizado(mapa, figura):
    n = 0
    for y in range(20):
        for x in range(20):
            drones_en_casilla = []
            for id, posicion in mapa.items():
                if posicion == (x, y):
                    drones_en_casilla.append(id)
            cantidad_drones = len(drones_en_casilla)
            n = max(n, cantidad_drones)  # Actualiza n con el máximo número de drones encontrados en una casilla
    
    longitud_maxima = max(len(cuadrado),(n))        
    figura_ajustada = {
        int(i): (x - 1, y - 1) for i, (x, y) in figura.items()
    }
    print(mapa)
    print(figura_ajustada)
    if(mapa != figura_ajustada):
        for x in range(20):
            # Imprimir número de la fila
            print(str(x+1).rjust(2), end=" ")
            for y in range(20):
                drones_en_casilla = []
                id_dron = None
                for id, posicion in mapa.items():
                    if posicion == (x, y):
                        drones_en_casilla.append(id)
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima-len(drones_en_casilla)) != 0:
                        print(FONDO_ROJO + numero_formateado + RESET, end=" "*(longitud_maxima))
                    else:
                        print(FONDO_ROJO + numero_formateado + RESET, end=" ")
                else:
                    cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                    print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
            print()  # Nueva línea para la siguiente fila
    else:
        encabezado = "*********** ART WITH DRONES **********"
        tablero_width = 20 * (longitud_maxima + 3)  # Tamaño total del tablero (20 filas, cada una con longitud_maxima y un espacio)
        encabezado_centralizado = encabezado.center(tablero_width)
        print(encabezado_centralizado)
        mensaje = "FIGURA COMPLETADA"
        mensaje_centralizado = mensaje.center(tablero_width)
        print(FONDO_CREMA + LETRA_GROSOR_NEGRITA + TEXTO_NEGRO + mensaje_centralizado + RESET)
        for x in range(20):
            # Imprimir número de la fila
            print(str(x+1).rjust(2), end=" ")
            for y in range(20):
                drones_en_casilla = []
                id_dron = None
                for id, posicion in mapa.items():
                    if posicion == (x, y):
                        drones_en_casilla.append(id)
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima-len(drones_en_casilla)) != 0:
                        print(FONDO_VERDE + TEXTO_ROJO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                    else:
                        print(FONDO_VERDE + TEXTO_ROJO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                else:
                    cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                    print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
            print()  # Nueva línea para la siguiente fila



#########     WEATHER   ###############

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def getWeather(ciudad, SERVER, PORT):
    while True:
        msg = ciudad
        with output_lock:
            if  (len(sys.argv) == 3):
                #SERVER = sys.argv[1]
                #PORT = int(sys.argv[2])
                ADDR = (SERVER, PORT)
                
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDR)
                print (f"Establecida conexión en [{ADDR}]")

                #msg=input("¿En que ciudad se va a actuar?")
                while msg != FIN :
                    print("Envio al servidor: ", msg)
                    send(msg, client)
                    print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
                    msg=input()
                print("Envio al servidor: ", FIN)
                send(FIN)
                client.close()
            else:
                print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Ciudad>")

        # Crear una matriz 2D de 20x20 posiciones para representar el espacio aéreo
            espacio_aereo = [[0 for _ in range(20)] for _ in range(20)]

        # Imprimir la matriz para visualizar el espacio aéreo
            for fila in espacio_aereo:
                print(fila)

        time.sleep(10)

#####    INFO FIGURA   #######

def getFigura():
    datos = {}
    with open("Figura.txt", 'r') as file:
        for linea in file:
            partes = linea.strip().split()
            if len(partes) == 3:
                ID = partes[0]
                x = int(partes[1])
                y = int(partes[2])
                datos[ID] = (x, y)
    return datos


def getCoord(id):
    datos = getFigura()
    if id in datos:
        coord = datos[id]
        print(f"X: {coord[0]}, Y: {coord[1]}")
    else:
        print("No se han encontrado el ID en la figura.")


#########  DRONE  ###########


class Consumer1(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000)

        consumer2.subscribe(['topic_a'])

        while not self.stop_event.is_set():
            for message in consumer2:
                print(pickle.loads(message.value))

                if self.stop_event.is_set():
                    break

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):    
        while True:
            info = getFigura()
            producer1 = KafkaProducer(bootstrap_servers='localhost:9092')
            data1 = pickle.dumps(info)
            producer1.send('topic_b', data1)
            
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            data = pickle.dumps("Mapa")
            producer.send('topic_b', data)

            print("Engine enviando coord destino y mapa")
            
            producer.close()
            time.sleep(4)


########## MAIN ##########

def main(argv = sys.argv):
    PORT = argv[0]
    MAX_Drones = argv[1]
    Server_Kafka = argv[2]
    Port_Kafka = argv[3]
    Server_W = argv[4]
    Port_W = argv[5]


    tam = len(argv)
    print(tam)

    datos = getFigura()


    try:
        if tam == 6:
            print(f'{argv[1]}')
            """
            ###  WEATHER  ###
            ADDR = (Server_W, Port_W)
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDR)
            print (f"Establecida conexión en [{ADDR}]")

            msg=sys.argv[3]
            contador = 0
            while contador < 10:
                print("Envio al servidor: ", msg)
                send(msg)
                print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
                
                # Pausar el programa durante el tiempo de consulta antes de enviar la próxima consulta
                time.sleep(10)
                contador += 1
            client.close()
            """

        ###  KAFKA  ###

        tasks = [Consumer1(), Producer()]

        for t in tasks:
            t.start()

        while True:

            time.sleep(1)

    except:
        pass

if __name__ == "__main__":
    main(sys.argv[1:])
    pass


    #  python3 AD_Engine.py 5050 20 127.0.0.1 9092 127.0.0.1 5050