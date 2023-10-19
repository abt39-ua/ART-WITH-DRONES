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
import signal

mapa = [[0 for _ in range(20)] for _ in range(20)]
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
SERVER = socket.gethostbyname(socket.gethostname())
MAX_CONEXIONES = 10

temp =0
output_lock = threading.Lock()

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    # Borrar el archivo registro.txt al cerrar el servidor
    # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)

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
    global mapa
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def actualizar_mapa(self, position_data):
        id, (x, y) = position_data
        if 0 <= x < 20 and 0 <= y < 20:
            mapa[y][x] = 1

    def run(self):
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000)

        consumer2.subscribe(['topic_a'])

        while not self.stop_event.is_set():
            for message in consumer2:
                try:
                    position_data = pickle.loads(message.value)
                    if isinstance(position_data, tuple) and len(position_data) == 2:
                        self.actualizar_mapa(position_data)
                        print(position_data)
                    else:
                        print("Datos inválidos recibidos del dron.")
                except Exception as e:
                    print(f"Error al procesar el mensaje: {e}")

                if self.stop_event.is_set():
                    break

class Producer(threading.Thread):
    global mapa
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while True:
            info = getFigura()
            
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            data = pickle.dumps(info)
            producer.send('topic_b', data)
            
            mapa_serializado = pickle.dumps(mapa)
            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            producer.send('topic_b', mapa_serializado)

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

    datos = getFigura()
    print(datos)

    try:
        if tam == 5:
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

        ###  KAFKA  ###

        tasks = [Consumer1(), Producer()]

        for t in tasks:
            t.start()

        while True:

            time.sleep(1)

    except:
        pass

# Crear un mapa de 20x20 inicializado con ceros (posición vacía)


if __name__ == "__main__":
    main(sys.argv[1:])
    pass


    #  python3 AD_Engine.py 5050 20 127.0.0.1 5050 127.0.0.1 5050