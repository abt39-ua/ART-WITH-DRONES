import json
import socket
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
import time
import threading

ID=0
HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
X = -1
Xi = 0
Y = -1
Yi = 0

#######  REGISTRY   ######

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry(ADDR):
    msg = input("Introduce tu alias")
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    print("Realizando solicitud al servidor")
    send(msg, client)
    ID = client.recv(2048).decode(FORMAT)
    print(f"Recibo del Servidor: {ID}")
 
    client.close()

######  DRONE   ######

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
    Xi = 0
    Yi = 0

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
                info = {"El Drone ": {ID}, " ha llegado a su destino: (": {X}, ", ": {Y}, ")": {}}
                print("He llegado a mi destino")
                limpiar()
                self.stop()

            else:
                info = {"Posición a la que se mueve el drone ": {ID}, ": (": {Xi}, ", ": {Yi}, ")": {}}
                print(f"Me he movido a ({Xi}, {Yi})")

            data = pickle.dumps(info)
            producer.send('topic_a', data)

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
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000)

        consumer2.subscribe(['topic_b'])

        while not self.stop_event.is_set():
            for message in consumer2:
                data = pickle.loads(message.value)

                if isinstance(data, dict):
                    for id, (x, y) in data.items():
                        if ID == id:
                            print(f'ID: {id}, Coordenadas:({x}, {y})')
                            setCoords(x,y)
                else:    
                    print(pickle.loads(message.value))

                if self.stop_event.is_set():
                    break



########## MAIN ##########

def resval():
    Xi = 0
    Yi = 0

def main(argv = sys.argv):
    if len(sys.argv) != 7:
        print("Error: El formato debe ser el siguiente: [IP_Engine] [Puerto_Engine] [IP_Broker] [Puerto_Broker] [IP_Registry] [Puerto_Registry]")
        sys.exit(1)
    else:  
        global ADDR, ad_engine_ip, ad_engine_port, broker_ip, broker_port, ad_registry_ip, ad_registry_port
        
        ad_engine_ip = sys.argv[1]
        ad_engine_port = int(sys.argv[2])
        broker_ip = sys.argv[3]
        broker_port = int(sys.argv[4])
        ad_registry_ip = sys.argv[5]
        ad_registry_port = int(sys.argv[6])
        orden = ""
        while(orden != "3"):
            
            print("¿Qué deseas hacer?")
            print("1. Registrarse")
            print("2. Empezar representación")
            print("3. Apagarse")
            orden = input()
      

            while orden != "1" and orden != "2" and orden != "3":
                print("Error, indica una de las 3 posibilidades por favor(1, 2 o 3).")
                orden = input()

            if orden == "1":
                if(ID != 0):
                    print("Ya estás registrado!")
                else:
                    ADDR = (ad_registry_ip, ad_registry_port)
                    registry(ADDR)

            if orden == "2":
                tasks = [Consumer(), Producer()]

                for t in tasks:
                    t.start()

                while True:
                    time.sleep(1)

            if orden == "3":
                sys.exit()


if __name__ == "__main__":
    main(sys.argv[1:])
