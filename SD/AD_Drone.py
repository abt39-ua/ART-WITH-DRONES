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
x = 0
y = 0


#######  REGISTRY   ######

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry():
    msg =input("Introduce tu alias")
    #if  (len(sys.argv) == 4):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

        #msg=sys.argv[3]
    while msg != FIN :
        print("Realizando solicitud al servidor")
        send(msg, client)
        ID = client.recv(2048).decode(FORMAT)
        #print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
        print(f"Recibo del Servidor: {ID}")
        #ID = client.recv(2048).decode(FORMAT)
        msg=input()

    print(f"{ID}")
    print ("SE ACABO LO QUE SE DABA")
    print("Envio al servidor: ", FIN)
    send(FIN)
    client.close()
    #else:
    #    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Alias deseado>")


#######   ENGINE   #######

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while True:
            producer = KafkaProducer(bootstrap_servers="localhost:9092")
            data = pickle.dumps("Posición a la que me muevo")
            producer.send('topic_a', data)

            print("Me he movido")

            producer.close()
            time.sleep(4)

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        grupo = '1'
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000, group_id = grupo)

        consumer2.subscribe(['topic_b'])

        while not self.stop_event.is_set():
            for message in consumer2:
                print(pickle.loads(message.value))

                if self.stop_event.is_set():
                    break
                #print("Leyendo mensajes de entrypark")



########## MAIN ##########

def main(args):
    print("¿Qué deseas hacer?")
    print("1. Registrarse")
    print("2. Empezar representación")
    print("3. Apagarse")
    orden = input()

    while orden != "1" and orden != "2" and orden != "3":
        print("Error, indica una de las 3 posibilidades por favor(1, 2 o 3).")
        orden = input()

    if orden == "1":
        registry()
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