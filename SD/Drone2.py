
import json
import socket
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
import time
import threading

ID=0
alias = ""

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
X = -1
Xi = 0
Y = -1
Yi = 0
######  DRONE   ######



#######  REGISTRY   ######

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry():
    global ID
    if  (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (SERVER, PORT)
        alias = sys.argv[3]
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

        #msg=sys.argv[3]
    print("Realizando solicitud al servidor")
    send(alias, client)
    ID = int(client.recv(2048).decode(FORMAT))
    #print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
    print(f"Recibo del Servidor: {ID}")
    #ID = client.recv(2048).decode(FORMAT)

    client.close()
    #else:
    #    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Alias deseado>")

#####

def setCoords(x, y):
    global X, Y
    X = x
    Y = y
    mov(X, Y, Xi, Yi)
    return X, Y

def mov(x, y, xi, yi):
    global Xi, Yi
    if x == xi and y == yi:
        print("He llegado")
        return FIN
    else:
        while xi != x or yi != y:
            dx = x-xi
            dy = y-yi

            if dx > 0:
                xi+=1
            elif dx < 0:
                xi-=1

            if dy > 0:
                yi += 1
            elif dx < 0:
                yi -= 1
            Xi = xi
            Yi = yi
            return Xi, Yi
        

def limpiar():
    global Xi, Yi
    Xi = 1
    Yi = 1

#######   ENGINE   #######

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            producer = KafkaProducer(bootstrap_servers="localhost:9092")

            if X == Xi and Y == Yi:
                position_data = (ID, (X, Y))
                print("He llegado a mi destino")
                self.stop()

            else:
                position_data = (ID, (X, Y))
                print(f"Me he movido a ({Xi+1}, {Yi+1})")

            serialized_position = pickle.dumps(position_data)
            producer.send('topic_a', serialized_position)

            producer.close()
            time.sleep(4)


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        coord = {}
        consumer1 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000)

        consumer1.subscribe(['topic_b'])
        consumer2.subscribe(['topic_c'])
        
        while not self.stop_event.is_set():
            for message in consumer1:
                mapa = pickle.loads(message.value)
                for fila in mapa:
                    print(' '.join(str(valor) for valor in fila))
            for message in consumer2:
                if isinstance(data, dict):
                    for id, (x, y) in data.items():
                        if ID == int(id):
                            print(f'ID: {id}, Coordenadas:({x-1}, {y-1})')
                            setCoords(x-1,y-1)
                            time.sleep(4)

                if self.stop_event.is_set():
                    break



########## MAIN ##########

def resval():
    Xi = 0
    Yi = 0 

########## MAIN ##########

def main(argv = sys.argv):
    limpiar()
    registry()

    tasks = [Consumer(), Producer()]

    for t in tasks:
        t.start()

    while True:
        time.sleep(1)

if __name__ == "__main__":
  main(sys.argv[1:])
