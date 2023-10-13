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
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
FORMAT = 'utf-8'
FIN = "FIN"
SERVER = socket.gethostbyname(socket.gethostname())
MAX_CONEXIONES = 10

temp =0
output_lock = threading.Lock()

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
                id = partes[0]
                x = float(partes[1])
                y = float(partes[2])
                datos[id] = (x, y)
    return datos

""""
def getCoord(id):
    if id in datos:
        coord = datos[id]
        print(f"X: {coord[0]}, Y: {coord[1]}")
    else:
        print("No se han encontrado el ID en la figura.")

"""

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
                #print("Leyendo mensajes de entrypark")

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while True:
            info = getFigura()
            for id, (x,y) in info.items():
                producer1 = KafkaProducer(bootstrap_servers='localhost:9092')
                data1 = pickle.dumps("{id} {x} {y}")
                producer1.send('topic_b', data1)

            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            data = pickle.dumps("Mapa")
            producer.send('topic_b', data)

            print("Engine enviando mapa")
            
            producer.close()
            time.sleep(3)


########## MAIN ##########

def main(argv = sys.argv):
    """"
    PORT = argv[1]
    MAX_Drones = argv[2]
    Server_Kafka = argv[3]
    Port_Kafka = argv[4]
    Server_W = argv[5]
    Port_W = argv[6]
    """

    print("Obteniendo Clima")
  
    
    print("Conecatnado con Dron")

    tam = len(argv)

    try:
        if tam == 7:
            ADDR = (Server_W, Port_W)

        tasks = [getWeather(), Consumer1(), Producer()]

        for t in tasks:
            t.start()

        while True:

            time.sleep(1)

    except:
        pass

    msg=input("¿En que ciudad se va a actuar? ")
    ciudad = msg
    print("Consiguiendo información del tiempo...")
    #getWeather()
    thread =threading.Thread(target = getWeather, args = (ciudad, ))
    thread.daemon = True
    thread.start()
    while True:

        print("Consiguiendo figura...")
        datos = {}
        #getFigura("Figura.txt")
        time.sleep(60)
        #connectDron()

if __name__ == "__main__":
    main(sys.argv[1:])