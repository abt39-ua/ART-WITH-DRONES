#-Conexión con weather
#-conexión con el fichero de dibujo y obtención num de drones
#-comprobar num drones en registro.txt
#-enviar cada pos requerida a cada dron
#-expresion del mapa a cada mov de dron

import tkinter as tk
import socket
import sys
import threading
import time
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pickle
import signal


mapa = {}
HEADER = 64
FORMAT = 'utf-8'
ad_engine_port = 0
MAX_Drones = 0
broker_ip = ""
broker_port = 0
ad_weather_ip = ""
ad_weather_port = 0
ciudades = {}
drones_positions_lock = threading.Lock()

consumer_thread = None
producer_thread = None

temp =0
output_lock = threading.Lock()

def obtener_nombre_ciudades():
    global ciudades
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

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    if consumer_thread:
        consumer_thread.stop()
    if producer_thread:
        producer_thread.stop()
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
    
#HAY QUE CORREGIRLO
def getWeather(ciudad, SERVER, PORT):
    while True:
        msg = ciudad
        with output_lock:
            if  (len(sys.argv) == 3):
                ADDR = (Ad, PORT)
                
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDR)

                obtener_nombre_ciudades()
                contador = 0
                ciudad_random = random.choice(list(ciudades.keys()))
                
                send(ciudad_random, client)
                print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
                client.close()

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


#########  DRONES MAPA  ###########
root = tk.Tk()
root.title("ART WITH DRONES")

titulo = tk.Label(root, text="ART WITH DRONES", font=("Arial", 16, "bold"), pady=10, bg="black", fg="white")
titulo.pack()
# Crear un lienzo para dibujar el mapa
canvas = tk.Canvas(root, width=600, height=600, bg="white")
canvas.pack()



# Función para dibujar el mapa con las coordenadas
def dibujar_mapa(mapa):
    canvas.delete("casilla")  # Limpiar solo las casillas antes de redibujar el mapa
    tamaño_casilla = 30  # Tamaño de cada casilla en píxeles
    
    for x in range(20):
        for y in range(20):

            x1 = x * tamaño_casilla
            y1 = y * tamaño_casilla
            x2 = x1 + tamaño_casilla
            y2 = y1 + tamaño_casilla
            # Dibujar la casilla delimitada por una línea negra
            canvas.create_rectangle(x1, y1, x2, y2, fill="white", outline="black", tags="casilla")
          
  
    for x in range(20):
        for y in range(20):
            x1 = x * tamaño_casilla
            y1 = y * tamaño_casilla
            x2 = x1 + tamaño_casilla
            y2 = y1 + tamaño_casilla
            
            ids_drones = [id_dron for id_dron, coordenadas in mapa.items() if coordenadas == (x, y)]
            if ids_drones:
                # Convertir los IDs de drones a una cadena separada por comas
                ids_str = ", ".join(map(str, ids_drones))
                # Dibujar el ID del dron en la casilla
                centro_x = (x1 + x2) // 2
                centro_y = (y1 + y2) // 2
                texto = canvas.create_text(centro_x, centro_y, text=ids_str, fill="red", font=("Arial", 10, "bold"), tags="casilla")
                # Obtener las coordenadas del texto en el lienzo
                x, y, _, _ = canvas.bbox(texto)
    
                # Ajustar el tamaño de fuente si el texto se sale de la casilla
                while x1 + 5 > x or x2 - 5 < x or y1 + 5 > y or y2 - 5 < y:
                    tamaño_fuente = int(canvas.itemcget(texto, "font").split()[1])
                    tamaño_fuente -= 1
                    canvas.itemconfig(texto, font=("Arial", tamaño_fuente, "bold"))
                    x, y, _, _ = canvas.bbox(texto)
            else:
                pass
          
dibujar_mapa({})

class Consumer(threading.Thread):
    global mapa
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        #self.mapa_lock = threading.Lock()  # Crear un objeto de bloqueo

    def stop(self):
        self.stop_event.set()

    def actualizar_mapa(self, position_data):
        try:
            id, (x, y) = position_data
        except (TypeError, ValueError):
            print("Datos de posición inválidos recibidos del dron.")
            return # Adquirir el bloqueo antes de realizar actualizaciones en el mapa
        
        # Adquirir el Lock antes de modificar drones_positions
       # drones_positions_lock.acquire()

        try:
            mapa[id] = (x, y)
            print(f"X: {x}, Y: {y}")
        finally:
            pass
            # Liberar el Lock sin importar si ocurre una excepción
           # drones_positions_lock.release()


    def run(self):
        try:
            consumer = KafkaConsumer(bootstrap_servers= self.broker_address,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000)

            consumer.subscribe(['topic_a'])

            while not self.stop_event.is_set():
                for message in consumer:
                    try:
                        position_data = pickle.loads(message.value)
                        if isinstance(position_data, tuple) and len(position_data) == 2:
                            self.actualizar_mapa(position_data)
                            print(position_data)
                            print(mapa)
                            root.after(10, dibujar_mapa, mapa)
                        else:
                            print("Datos inválidos recibidos del dron.")
                    except Exception as e:
                        print(f"Error al procesar el mensaje: {e}")

                    if self.stop_event.is_set():
                        break
               
        except Exception as e:
            print(f"Error en el consumidor: {e}")
        

class Producer(threading.Thread):
    global mapa
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.mapa_lock = threading.Lock()  # Objeto de bloqueo para proteger el mapa


    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            datos = getFigura()
            
            producer_coor = KafkaProducer(bootstrap_servers=self.broker_address )
            coordenadas = pickle.dumps(datos)
            producer_coor.send('topic_coord', coordenadas)
            
            time.sleep(3)
            producer_mapa = KafkaProducer(bootstrap_servers=self.broker_address)
            mapa_serializado = pickle.dumps(mapa)
            producer_mapa.send('topic_mapa', mapa_serializado)
            
            producer_coor.close()
            producer_mapa.close()
            pass

########## MAIN ##########

def main(argv = sys.argv):
    global ad_engine_port, MAX_Drones, broker_ip, broker_port, ad_weather_ip, ad_weather_port, mapa
    ad_engine_port = argv[0]
    MAX_Drones = argv[1]
    broker_ip = argv[2]
    broker_port = argv[3]

    tam = len(argv)

    datos = getFigura()
    print(datos)

    try:
        if tam == 7:
            
            ad_engine_port = argv[0]
            MAX_Drones = argv[1]
            broker_ip = argv[2]
            broker_port = argv[3]
            ad_weather_ip = argv[4]
            ad_weather_port = argv[5]
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

        tasks = [Consumer(), Producer()]
        # Acceder a la instancia del consumidor desde la lista de tareas
        consumer_thread = tasks[0]
        producer_thread = tasks[1]
        for t in tasks:
            t.start()

        # Iniciar el bucle principal de la interfaz gráfica
        root.mainloop()
        while True:

            time.sleep(1)

    except:
        pass

# Crear un mapa de 20x20 inicializado con ceros (posición vacía)


if __name__ == "__main__":
    main(sys.argv[1:])
    pass


    #  python3 AD_Engine.py 5050 20 127.0.0.1 5050 127.0.0.1 5050