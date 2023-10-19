import socket
import sys
import time
import signal
import random
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import pickle
import threading

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ciudades = {}

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
            producer1 = KafkaProducer(bootstrap_servers='localhost:9092')
            data1 = pickle.dumps("Coord destino")
            producer1.send('topic_b', data1)

            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            data = pickle.dumps("Mapa")
            producer.send('topic_b', data)

            print("Engine enviando mapa")
            
            producer.close()
            time.sleep(3)

def obtener_nombre_ciudades():
    try:
        # Abrimos el archivo en modo lectura
        with open('ciudades.txt', 'r') as archivo:
            for linea in archivo:
                # Dividimos la línea en ciudad y grados usando el carácter ':'
                ciudad, grados = linea.strip().split(':')
                # Guardamos la ciudad y grados en el diccionario
                ciudades[ciudad] = grados
    except FileNotFoundError:
        return "Error: El archivo 'ciudades.txt' no se encuentra.", None
    except Exception as e:
        return f"Error: {e}", None

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando conexión...")
    client.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)

def procesar_archivo_registro():
    try:
        # Abrimos el archivo en modo lectura
        with open('registro.txt', 'r') as archivo:
            for linea in archivo:
                for linea in archivo:
                    # Eliminamos los caracteres especiales y dividimos la línea en partes
                    partes = linea.strip().split()
                    # Verificamos si hay suficientes partes en la línea
                    if len(partes) == 3:
                        ID, x, y = partes
                        print(f"ID: {ID}, Coordenada X: {x}, Coordenada Y: {y}")
    except FileNotFoundError:
        return "Error: No hay drones registrados.", None
    except Exception as e:
        return f"Error: {e}", None

    
########## MAIN ##########8


def start():
    
    if  (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (SERVER, PORT)
        try:
            tiempo_de_consulta = int(sys.argv[3])
        except ValueError:
            print("El tiempo de consulta debe ser un número entero.")
            sys.exit()
        
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")
        obtener_nombre_ciudades()
        contador = 0
        ciudad_random = random.choice(list(ciudades.keys()))
        msg = ciudad_random
        while contador < 10:
            print("Envio al servidor: ", msg)
            send(msg)
            print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
            # Pausar el programa durante el tiempo de consulta antes de enviar la próxima consulta
            time.sleep(tiempo_de_consulta)
            contador += 1

        client.close()

    else:
        print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Ciudad> <Tiempo consulta>")

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
        
start()
# Crear una matriz 2D de 20x20 posiciones para representar el espacio aéreo
# espacio_aereo = [[0 for _ in range(20)] for _ in range(20)]

# Imprimir la matriz para visualizar el espacio aéreo
# for fila in espacio_aereo:
    # print(fila)