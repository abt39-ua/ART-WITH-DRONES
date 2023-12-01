import socket
import sys
import threading
import time
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
import pickle
import signal
import random
import re
import json
import requests
from pymongo import MongoClient, server_api

registrados = 0
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
figura = {}
figuras = {}
volver_base = {}
actuacion = True
drones_positions_lock = threading.Lock()
completada = False
consumer_thread = None
producer_thread = None
t_consulta = 0
temp =0
output_lock = threading.Lock()
ciudad = ""

# Conexión a MongoDB con una versión específica de la API del servidor
uri = "mongodb://localhost:27018"
api_version = server_api.ServerApi('1', strict=True, deprecation_errors=True)
client = MongoClient(uri, server_api=api_version)
dbName = 'SD'
colletionName = 'drones'

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
# Conectar al servidor de MongoDB
client.admin.command('ismaster')

# Obtener la base de datos
db = client['SD']

# Obtener la colección
collection = db['drones']

def conectar_db():
    # Conectar a la instancia de MongoDB
    client = MongoClient('localhost', 27018)  # Asegúrate de cambiar el puerto si es diferente
    # Seleccionar la base de datos "SD"
    db = client['SD']
    # Seleccionar la colección "drones"
    collection = db['drones']
    return collection

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

############  WEATHER   ##################3

def obtener_nombre_ciudades():
    global ciudad
    try:
        with open('ciudades.txt', 'r') as archivo:
            ciudadesN = [line.strip() for line in archivo]
            ciudad = random.choice(ciudadesN)
    except FileNotFoundError:
        return "Error: El archivo 'ciudades.txt' no se encuentra.", None
    except Exception as e:
        return f"Error: {e}", None
    
def getTemperatura(ciudad):
    global temp
    url = "https://api.openweathermap.org/data/2.5/weather?q={}&appid=274d9ed11cbef3a98393a23a34f79bb7&units=metric".format(ciudad)
    res = requests.get(url)
    data = res.json()

    temp = data["main"]["temp"]

def getWeather():
    if ciudad == "":
        obtener_nombre_ciudades()
    ciudad_random = ciudad

    getTemperatura(ciudad)
    temperatura =  temp
    print("Ciudad y temperatura:", ciudad, temperatura)

    if temperatura >= 0:
        return True
    else:
        return False
    
def consultar_clima():
    global actuacion
    while True:
        actuacion = getWeather()
        time.sleep(int(t_consulta))

##############  FIGURAS  #####################

def obtener_registrados_desde_db():
    collection = conectar_db()
    return collection.count_documents({})

def procesar_registrados():
    global registrados
    while True:
        registrados_db = obtener_registrados_desde_db()
        if registrados_db >= len(figura):
            registrados = registrados_db
            print("Todos los drones están registrados.")
            break
        else:
            print("Esperando a que todos los drones se registren...")
            time.sleep(1)

def getFiguras():
    global figuras
    try:
        with open("Figuras.json", "r") as file:
            data = json.load(file)
            figuras_data = data.get("figuras", [])
            for figura_data in figuras_data:
                nombre = figura_data.get("Nombre")
                drones_data = figura_data.get("Drones", [])
                drones_positions = {}
                for drone in drones_data:
                    ID = drone.get("ID")
                    pos = drone.get("POS", "0,0").split(",")
                    x, y = map(int, pos)
                    drones_positions[ID] = (x, y)
                figuras[nombre] = drones_positions
            return figuras
    except FileNotFoundError:
        print("Error: El archivo 'figuras.json' no se encuentra.")
    except Exception as e:
        print(f"Error al leer el archivo JSON: {e}")
    return None

def getFigura():
    global figura, figuras
    # Obtener la primera figura del diccionario de figuras
    if figuras:
        nombre_figura = next(iter(figuras))
        drones = figuras[nombre_figura]
        figura = {}
        for ID, pos in drones.items():
            x, y = pos
            figura[ID] = (x, y)
        # Eliminar la figura procesada del diccionario de figuras
        del figuras[nombre_figura]
        print("Figura procesada y guardada en figura:")
        #print(figura)
    else:
        print("No hay más figuras para procesar.")

##############   MAPA    #######################

# Códigos de escape ANSI para colores de fondo
FONDO_ROJO = "\033[41m"
FONDO_VERDE = "\033[102m"
FONDO_CREMA = "\033[48;5;224m"
RESET = "\033[0m"
TEXTO_NEGRO = "\033[30m"
LETRA_GROSOR_NEGRITA = "\033[1m"


cuadrado = "□"


def imprimir_mapa_actualizado(mapa, figura):
    global completada
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
    #print(mapa)
    #print(figura_ajustada)
    #print(longitud_maxima)
    # Imprimir línea de números del 1 al 20
    if(mapa == figura_ajustada):
        completada = True
        encabezado = "*********** ART WITH DRONES **********"
        tablero_width = 20 * (longitud_maxima + 3)  # Tamaño total del tablero (20 filas, cada una con longitud_maxima y un espacio)
        encabezado_centralizado = encabezado.center(tablero_width)
        print(encabezado_centralizado)
        mensaje = "FIGURA COMPLETADA"
        mensaje_centralizado = mensaje.center(tablero_width)
        print(FONDO_CREMA + LETRA_GROSOR_NEGRITA + TEXTO_NEGRO + mensaje_centralizado + RESET)

    if(longitud_maxima > 2):
        print("   " + "  ".join(str(i).rjust(longitud_maxima*2-2) for i in range(1, 21)))
    else:
        print("   " + " ".join(str(i).rjust(3) for i in range(1, 21)))
    
    for x in range(20):
        # Imprimir número de la fila
        print(str(x+1).rjust(2), end=" ")
        for y in range(20):
            drones_en_casilla = []
            id_dron = None
            for id, posicion in mapa.items():
                if posicion == (x, y):
                    drones_en_casilla.append(id)
                
            if(mapa == figura_ajustada):
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima != 1): 
                        if(longitud_maxima-len(drones_en_casilla)) != 0:
                            print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                        else:
                            print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                    else:
                        if(longitud_maxima-len(drones_en_casilla)) != 0:
                            print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                        else:
                            print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                else:
                    if(longitud_maxima != 1):
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                        print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
                    else:
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima*2)
                        print(cuadrado_formateado, end=" "*longitud_maxima*2)

            else:
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima != 1): 
                        if len(drones_en_casilla) == 1 and figura_ajustada.get(int(drones_en_casilla[0])) == (x, y):
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                        else:
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                    else:
                        if len(drones_en_casilla) == 1 and figura_ajustada.get(int(drones_en_casilla[0])) == (x, y):
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                        else:
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_ROJO + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(' ' + FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                else:
                    if(longitud_maxima != 1): 
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                        print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
                    else:
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima*2)
                        print(cuadrado_formateado, end=" "*longitud_maxima*2) 
        print()  # Nueva línea para la siguiente fila

##############   KAFKA   ######################

class Consumer(threading.Thread):
    global mapa, registrados
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
            #print(mapa)
            #print(figura)
            if len(mapa) == registrados == len(figura):# and all(id_dron in mapa for id_dron in figura.keys()):
                time.sleep(1)
                imprimir_mapa_actualizado(mapa, figura)
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
                            print(len(mapa))
                            print(registrados)
                            print(len(figura))
                        else:
                            print("Datos inválidos recibidos del dron.")
                    except Exception as e:
                        print(f"Error al procesar el mensaje: {e}")

                    if self.stop_event.is_set():
                        break
        except Exception as e:
            print(f"Error en el consumidor: {e}")
        


class Producer(threading.Thread):
    global mapa, registrados, figura, volver_base, t_consulta, actuacion, figuras
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.mapa_lock = threading.Lock()  # Objeto de bloqueo para proteger el mapa
    def stop(self):
        self.stop_event.set()

    def enviar_coordenadas_figura(self, drones_positions, producer_coor, producer_mapa):
        #print(drones_positions)
        
        global completada
        # Enviar coordenadas de la figura actual
        producer_coor.send('topic_coord', pickle.dumps(drones_positions))
        producer_mapa.send('topic_mapa', pickle.dumps(mapa))
        
        # Esperar a que los drones completen la figura actual
        time.sleep(5)  # Esperar 5 segundos
        
        # Verificar si la figura está completada en el mapa
        completada = self.verificar_completitud(mapa, drones_positions)

    def verificar_completitud(self, mapa, coordenadas_figura):
        global completada
        #print(mapa)
        figura_ajustada = {
            int(i): (x - 1, y - 1) for i, (x, y) in coordenadas_figura.items()
        }
        #print(figura_ajustada)
        print(completada)
        if all(key in mapa and mapa[key] == value for key, value in figura_ajustada.items()):
            return True
        else:
            return False

    def run(self):
        global completada
        # Inicia un hilo para consultar el clima cada t_consulta segundos
        thread_consulta = threading.Thread(target=consultar_clima)
        thread_consulta.start()
        print(t_consulta)
        getFiguras()
        getFigura()
        #print(figura)
        #print(len(figura))
        volver_base = {id: (1, 1) for id in figura.keys()}
        while registrados < len(figura):
            time.sleep(1)
            procesar_registrados()
        while not self.stop_event.is_set():
            print("hola")
            if(completada == True):
                getFigura()
                print("segunda figura:")
                #print(figura)
                #print(mapa)
            while(mapa != figura):
                if len(mapa) == registrados == len(figura):
                    if(actuacion == True):
                        producer_coor = KafkaProducer(bootstrap_servers=self.broker_address )
                        producer_mapa = KafkaProducer(bootstrap_servers=self.broker_address)
                        self.enviar_coordenadas_figura(figura, producer_coor, producer_mapa)

                    else:
                        self.enviar_coordenadas_figura(volver_base,producer_coor, producer_mapa)
                        
                else:
                    pass
                if(completada == True):
                    "saliendoooo"
                    break
            # Llama a la función getWeather cada t_consulta segundos
                

########## MAIN ##########

def main(argv = sys.argv):
    print(len(sys.argv))
    if len(sys.argv) != 6:
        print("Error: El formato debe ser el siguiente: [Puerto_Engine] [N_Máximo_Drones] [IP_Broker] [Puerto_Broker] [IP_Weather] [Puerto_Weather] [Tiempo_Consulta]")
        sys.exit(1)
    else:
        global completada, ad_engine_port, MAX_Drones, broker_ip, broker_port, ad_weather_ip, ad_weather_port, t_consulta, mapa
        ad_engine_port = int(argv[0])
        MAX_Drones = argv[1]
        broker_ip = argv[2]
        broker_port = int(argv[3])
        t_consulta = argv[4]

        tam = len(argv)
        print(tam)

        try:

            ###  KAFKA  ###

            tasks = [Consumer(), Producer()]
            # Acceder a la instancia del consumidor desde la lista de tareas
            consumer_thread = tasks[0]
            producer_thread = tasks[1]
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


    #  python3 AD_Engine.py 5050 20 127.0.0.1 5050 5
