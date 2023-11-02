import json
import socket
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
import time
import threading
import signal

ID=0
alias = ""

HEADER = 64

FORMAT = 'utf-8'
FIN = "FIN"
ca_x = 0
ca_y = 0
cd_x = 0
cd_y = 0
ad_engine_ip = ""
ad_engine_port = 0
broker_ip = ""
broker_port = 0
ad_registry_ip = ""
ad_registry_port = 0

semaforo = threading.Semaphore(value=1)

def handle_interrupt(signum, frame):
    global ID
    print(f"Cerrando conexión...")
    client.close()
    producer.close()
    sys.exit(0)

# Manejar la señal SIGINT (CTRL+C)
signal.signal(signal.SIGINT, handle_interrupt)

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry():
    global ID
    alias=input("Introduce tu alias: ")
    ADDR = (ad_registry_ip, ad_registry_port)  
        
    
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

# RETORNAR A LA CASILLA INICIAL
def volver():
    global cd_x, cd_y
    cd_x = 1
    cd_y = 1

#######   ENGINE   #######

class Producer(threading.Thread):
    global ID, ca_x, ca_y
    ca_x = 1
    ca_y = 1
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.last_sent_message = None  # Almacena el último mensaje enviado a Kafka

    def stop(self):
        self.stop_event.set()

    def run(self):
        global semaforo
        while not self.stop_event.is_set():
            producer = KafkaProducer(bootstrap_servers=self.broker_address)

            position_data = (ID, (ca_x, ca_y))
            if ca_x == cd_x and ca_y == cd_y:
                print("He llegado a mi destino")
            else:
                print(f"Me he movido a ({ca_x+1}, {ca_y+1})")

            if position_data != self.last_sent_message:
                semaforo.acquire()
                serialized_position = pickle.dumps(position_data)
                producer.send('topic_a', serialized_position)

                # Almacenar el nuevo mensaje como el último mensaje enviado
                self.last_sent_message = position_data

                # Liberar el permiso del semáforo
                semaforo.release()
            time.sleep(3)
            producer.close()

def setCoords(x, y):
    global cd_x, cd_y
    cd_x = x
    cd_y = y
    mov(cd_x, cd_y, ca_x, ca_y)

def mov(cd_x, cd_y, x, y):
    global ca_x, ca_y 
    ca_x = x
    ca_y = y
    if ca_x == cd_x and ca_y == cd_y:
        return 1
    else:
        distancia_x = cd_x-ca_x
        distancia_y = cd_y-ca_y

        if distancia_x > 0:
            ca_x+=1
        elif distancia_x < 0:
            ca_x-=1

        if distancia_y > 0:
            ca_y += 1
        elif distancia_y < 0:
            ca_y -= 1
        return 0

class Consumer(threading.Thread):
    global ID
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        try:
            consumer_coord = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            consumer_mapa = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            
            consumer_coord.subscribe(['topic_coord'])
            consumer_mapa.subscribe(['topic_mapa'])

            while not self.stop_event.is_set():
                print("Estoy consumiendo")
                for message in consumer_coord:
                    datos = message.value
                    for id, (x, y) in datos.items():
                        if ID == int(id):
                            print(f'ID: {id}, Coordenadas:({x}, {y})')
                            time.sleep(2)
                            
                            semaforo.acquire()
                            setCoords(x-1,y-1)
                            # Liberar el semáforo después de la actualización del mapa
                            semaforo.release()
                            time.sleep(4)
                if self.stop_event.is_set():
                    break

        except Exception as e:
            print(f"Error en el consumidor: {e}")

########## MAIN ##########

def main(argv = sys.argv):
    print(len(sys.argv))
    if len(sys.argv) != 7:
        print("Error: El formato debe ser el siguiente: [IP_Engine] [Puerto_Engine] [IP_Broker] [Puerto_Broker] [IP_Registry] [Puerto_Registry]")
        sys.exit(1)
    else:  
        global ad_engine_ip, ad_engine_port, broker_ip, broker_port, ad_registry_ip, ad_registry_port, ID
        
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
                    registry()

            if orden == "2":
                print(ID)
                if(ID == 0):
                    print("No estás registrado!")
                else:
                    tasks = [Consumer(), Producer()]

                    for t in tasks:
                        t.start()

                    while True:
                        time.sleep(1)

            if orden == "3":
                sys.exit()

if __name__ == "__main__":
  main(sys.argv[1:])
