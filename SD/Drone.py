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
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


#######  REGISTRY   ######

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando conexión...")
    client.close()  # Cierra el socket del servidor
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
        try:
            if  (len(sys.argv) == 4):
                SERVER = sys.argv[1]
                PORT = int(sys.argv[2])
                ADDR = (SERVER, PORT)
                client.connect(ADDR)
                print (f"Establecida conexión en [{ADDR}]")

                msg=sys.argv[3]
                print("Realizando solicitud al servidor")
                send(msg, client)
                recibido = client.recv(2048)
                response_data = pickle.loads(recibido)
                ID, alias = response_data
                print("Recibo del Servidor: ID =", ID, ", Alias =", alias)
            else:
                print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Alias deseado>")
            
            grupo = ID
            
            consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, 
                                                group_id = grupo)
        
            consumer2.subscribe(['topic_b'])

            while not self.stop_event.is_set():
                for message in consumer2:
                    position_data = pickle.loads(message.value)
                    message_id = position_data['id']
                    x = position_data['x']
                    y = position_data['y']

                    if message_id == self.consumer_id:
                        print(f"Posición recibida para ID {self.consumer_id}: (x={x}, y={y})")
                        # Aquí puedes guardar la posición en la forma que desees
                        # por ejemplo, guardarla en una lista o base de datos.
                        # Ejemplo de guardar en una lista:
                        # posiciones.append((x, y))

                    if self.stop_event.is_set():
                        break

        except Exception as e:
            print(f"Error en hilo del consumidor: {e}")
            # Puedes manejar el error aquí, por ejemplo, salir del bucle o tomar medidas específicas según el error.
        finally:
            # Realiza las tareas de limpieza, si es necesario
            print("Hilo del consumidor terminado.")


tasks = [Consumer(), Producer()]

for t in tasks:
    t.start()

while True:

    time.sleep(1)