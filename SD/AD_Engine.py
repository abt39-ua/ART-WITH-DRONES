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
import datetime
from pymongo import MongoClient, server_api
from flask import Flask, jsonify

app = Flask(__name__)
registrados = 0
mapa = {}
HEADER = 64
FORMAT = 'utf-8'
ad_engine_port = 0
ad_engine_ip = ""
MAX_Drones = 0
broker_ip = ""
broker_port = 0
ad_weather_ip = ""
ad_weather_port = 0
ciudades = {}
figura = {}
figuras = {}
volver_base = {}
volver_base_ajustado = {}
actuacion = True
drones_positions_lock = threading.Lock()
completada = False
consumer_thread = None
producer_thread = None
t_consulta = 0
temp =0
output_lock = threading.Lock()
ciudad = ""
volver_base = {}
figura_ajustada = {}
auditoria = -1

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
    try:
        # Conectar a la instancia de MongoDB
        client = MongoClient('localhost', 27017)  # Asegúrate de cambiar el puerto si es diferente
        # Seleccionar la base de datos "SD"
        db = client['SD']
        # Seleccionar la colección "drones"
        collection = db['drones']
        return collection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {str(e)}")
        return None

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    if consumer_thread:
        consumer_thread.stop()
    if producer_thread:
        producer_thread.stop()
    with open("auditoria.txt", "a") as file:
        file.write("\n*El servidor núcleo se ha apagado.*\n")
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
                    estado = True  # Añadir variable de estado por defecto
                    drones_positions[ID] = {"posicion": (x, y), "estado": estado}
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
        for ID, info in drones.items():
            x, y = info["posicion"]
            estado = True  # Añadir variable de estado por defecto
            figura[ID] = {"posicion": (x, y), "estado": estado}
        # Eliminar la figura procesada del diccionario de figuras
        del figuras[nombre_figura]
        print("Figura procesada y guardada en figura:")
        #print(figura)
        return nombre_figura
    else:
        return ""

##############   MAPA    #######################
"""
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
    figura_ajustada = {}
    for drone_id, drone_info in figura.items():
            nueva_posicion = (drone_info['posicion'][0] - 1, drone_info['posicion'][1] - 1)

            nuevo_estado = drone_info['estado']
            
            # Crear la entrada correspondiente en volver_base_ajustado
            figura_ajustada[drone_id] = {'posicion': nueva_posicion, 'estado': nuevo_estado}

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
""" 
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
        collection = conectar_db()
        try:
            id, (x, y), estado = position_data
        except (TypeError, ValueError):
            print("Datos de posición inválidos recibidos del dron.")
            return 
       

        try:
            # Actualizar la posición del dron en la base de datos
            filter_query = {"_id": id}
            update_query = {"$set": {"posicion_x": x, "posicion_y": y, "estado": estado}}
            collection.update_one(filter_query, update_query)
           
            mapa[id] = ((x, y), estado)
            print(mapa)
            if len(mapa) == registrados == len(figura):
                time.sleep(1)
                #imprimir_mapa_actualizado(mapa, figura)
                
        except Exception as e:
            print(f"Error al actualizar la posición en la base de datos: {str(e)}")
    
        finally:
            pass

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
                        if isinstance(position_data, tuple) and len(position_data) == 3:
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
    global mapa, registrados, figura, volver_base, t_consulta, actuacion, figuras, volver_base_ajustado
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.mapa_lock = threading.Lock()  # Objeto de bloqueo para proteger el mapa
    def stop(self):
        self.stop_event.set()

    def enviar_coordenadas_figura(self, drones_positions, producer_coor, producer_mapa):
        global completada
        # Enviar coordenadas de la figura actual
        producer_coor.send('topic_coord', pickle.dumps(drones_positions))
        producer_mapa.send('topic_mapa', pickle.dumps(mapa))
        
        # Esperar a que los drones completen la figura actual
        time.sleep(5)  # Esperar 5 segundos
        
        # Verificar si la figura está completada en el mapa
        completada = self.verificar_completitud(mapa, drones_positions)

    def verificar_completitud(self, mapa, coordenadas_figura):
        global completada, figura_ajustada
        print(mapa)
        print(coordenadas_figura)
        estado_defecto = True
        
        for drone_id, drone_info in coordenadas_figura.items():
            nueva_posicion = (drone_info['posicion'][0] - 1, drone_info['posicion'][1] - 1)
            nuevo_estado = True
            
            # Crear la entrada correspondiente en volver_base_ajustado
            figura_ajustada[drone_id] = (nueva_posicion,nuevo_estado)

        print(completada)
        print("Imprimiendo mapa:")
        print(mapa)
        print("Imprimiendo figura_ajustada")
        print(figura_ajustada)
        if all(key in mapa and mapa[key] == value for key, value in figura_ajustada.items()):
            if (figura_ajustada != volver_base_ajustado): 
                return True
            else:
                return False
        else:
           return False


    def run(self):
        global completada, auditoria
        # Inicia un hilo para consultar el clima cada t_consulta segundos
        thread_consulta = threading.Thread(target=consultar_clima)
        thread_consulta.start()
        print(t_consulta)
        getFiguras()
        nombre_figura = getFigura()
        print(figura)
        print(figuras)
        print(auditoria)
        if figura != {}:
            try:
                if(auditoria == '0'):
                    with open("auditoria.txt", "w") as file:
                        file.write("\n*Nuevo servidor núcleo operativo.*\n")
                        pass
                    with open("auditoria.txt", "a") as file: 
                        # Obtener la dirección IP de tu máquina
                        file.write(f"Los drones se producirán en la IP: {broker_ip}\n")
            except Exception as e:
                print(f"Error al borrar el contenido del archivo de auditoría y escribir el mensaje: {e}")

        contador = 0
        aux = contador
        for drone_id, drone_info in figura.items():
            # Establecer las posiciones y estados por defecto
            nueva_posicion = (1, 1)
            nuevo_estado = False
            
            # Crear la entrada correspondiente en volver_base
            volver_base[drone_id] = {'posicion': nueva_posicion, 'estado': nuevo_estado}
        
        for drone_id, drone_info in volver_base.items():
                print(drone_info)
                nueva_posicion = (drone_info['posicion'][0] - 1, drone_info['posicion'][1] - 1)
                nuevo_estado = False
                
                # Crear la entrada correspondiente en volver_base_ajustado
                volver_base_ajustado[drone_id] = (nueva_posicion, nuevo_estado)

        while registrados < len(figura):
            time.sleep(1)
            procesar_registrados()
        while not self.stop_event.is_set():
            while(mapa != figura):
                
                if len(mapa) == len(figura) and registrados >= len(figura):
                    producer_coor = KafkaProducer(bootstrap_servers=self.broker_address)
                    producer_mapa = KafkaProducer(bootstrap_servers=self.broker_address)
                    if(actuacion == True):
                        if aux == contador:
                            aux += 1
                            inicio_evento = datetime.datetime.now()
                            inicio_evento_str = inicio_evento.strftime("%Y-%m-%d %H:%M:%S")

                            with open("auditoria.txt", "a") as file:
                                file.write(f"La figura que se va a representar es: {nombre_figura}\n")
                                file.write(f"Inicio del evento: {inicio_evento_str}\n")
                                
                        self.enviar_coordenadas_figura(figura, producer_coor, producer_mapa)

                    else:
                        self.enviar_coordenadas_figura(volver_base, producer_coor, producer_mapa)
                        
                else:
                    pass
                if actuacion == True and completada == True and mapa != volver_base_ajustado:
                    time.sleep(5)
                    with open("auditoria.txt", "a") as file:
                        fin_evento = datetime.datetime.now()
                        fin_evento_str = fin_evento.strftime("%Y-%m-%d %H:%M:%S")
                        file.write(f"EVENTO FINALIZADO - FECHA: {fin_evento_str}\n")
                        
                    nombre_figura = getFigura()
                    if(nombre_figura != ""):
                        with open("auditoria.txt", "a") as file:
                            file.write("\n*¡PREPARANDO EL SIGUIENTE EVENTO!*\n")
                        print("Procesando siguiente figura...")
                        print("Mostrando figura:")
                        print()
                        contador = aux
                    else: 
                        print("No hay más figuras para procesar:")
                        with open("auditoria.txt", "a") as file:
                            file.write("\n*¡ESPECTÁCULO FINALIZADO!*\n")
                        while(True):
                            self.enviar_coordenadas_figura(volver_base,producer_coor, producer_mapa)
                

            # Llama a la función getWeather cada t_consulta segundos
                

########## MAIN ##########

def main(argv = sys.argv):
    print(len(sys.argv))
    if len(sys.argv) != 7:
        print("Error: El formato debe ser el siguiente: [Puerto_Engine] [N_Máximo_Drones] [IP_Broker] [Puerto_Broker] [IP_Weather] [Puerto_Weather] [Tiempo_Consulta] [Auditoria]")
        sys.exit(1)
    else:
        global completada, ad_engine_port, MAX_Drones, broker_ip, broker_port, ad_weather_ip, ad_weather_port, t_consulta, mapa, auditoria
        ad_engine_port = int(argv[0])
        MAX_Drones = argv[1]
        broker_ip = argv[2]
        broker_port = int(argv[3])
        t_consulta = argv[4]
        auditoria = argv[5]
        print(auditoria)
        tam = len(argv)
        print(tam)

        # Borrar el contenido del archivo de auditoría
        try:
            if(auditoria == '0'):
                with open("auditoria.txt", "w") as file:
                    file.write("¡NO SE REGISTRARON EVENTOS!\n")
                    print("Contenido del archivo de auditoría borrado.")
        except Exception as e:
            print(f"Error al borrar el contenido del archivo de auditoría: {e}")


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


if __name__ == "__main__":
    # Ejecutar la lógica principal en el hilo principal
    main(sys.argv[1:])

# Iniciar la aplicación Flask en un hilo separado
    flask_thread = threading.Thread(target=app.run, kwargs={'port': 5001})
    flask_thread.start()
    #  python3 AD_Engine.py 5050 20 127.0.0.1 5050 5