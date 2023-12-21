#SAMUEL LORENZO
#ALICIA BAQUERO
import json
import socket
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
import time
import threading
import signal
import jwt
import time
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient, server_api, errors
from flask import Flask, jsonify, request
import hashlib

ID = 0
alias = ""

HEADER = 64

FORMAT = 'utf-8'
FIN = "FIN"
ca_x = 0
ca_y = 0
cd_x = -1
cd_y = -1
ad_engine_ip = ""
ad_engine_port = 0
broker_ip = ""
broker_port = 0
ad_registry_ip = ""
ad_registry_port = 0
estado = False
datos_coor = {}
token = ""
secret_key = '123'

semaforo = threading.Semaphore(value=1)

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
aux = 0
contador = 0
def handle_interrupt(signum, frame):
    global aux, contador
    if ((ca_x != 0 or ca_y != 0) and ID != 0):
        if(aux == contador):
            aux += 1
            with open("auditoria.txt", "a") as file:
                file.write(f"- ¡El dron con ID: {ID} se ha caído durante la actuación.!\n")
    else:
        if(ID != 0):
            with open("auditoria.txt", "a") as file:
                file.write(f"- El dron con ID: {ID} se ha apagado.\n")
    print(f"Cerrando conexión...")
    sys.exit(0)

# Manejar la señal SIGINT (CTRL+C)
signal.signal(signal.SIGINT, handle_interrupt)

def send(msg, client):

    message = str(msg).encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def verificar_tiempo_de_expiracion(token):
    global secret_key
    if(token):
        try:
            # Decodifica el token para obtener la información del tiempo de expiración
            decoded_token = jwt.decode(token.encode(), key=secret_key, algorithms=['HS256'])
            
            # Obtiene el tiempo de expiración del token
            tiempo_expiracion = datetime.utcfromtimestamp(decoded_token['exp'])
            
            # Obtiene el tiempo actual
            tiempo_actual = datetime.utcnow()

            # Definir un timedelta de 20 segundos
            delta_20_seconds = timedelta(seconds=20)
            
            # Verificar si ha expirado
            if tiempo_actual - tiempo_expiracion > delta_20_seconds:
                return True  # Token ha expirado
            else:
                return False   # Token aún es válido

        except jwt.ExpiredSignatureError:
            return True      # Token ha expirado

        except jwt.InvalidTokenError:
            return True      # Token no válido
    else:
        print("Error: Token no válido.")
        return True

def solicitar_nuevo_token_al_servidor():
    global token
    # Lógica para solicitar un nuevo token al servidor de registro
    # ...
    ADDR = (ad_registry_ip, ad_registry_port)
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)

        print("Realizando solicitud de token al servidor")
        send(ID, client)
        mensaje = client.recv(2048).decode(FORMAT)
        data = json.loads(mensaje)
        if 'token' in data:
            token = data['token']
            print(f"Nuevo token recibido: {token}")
        else:
            print("No se recibió un token en la respuesta.")

    except (ConnectionRefusedError, TimeoutError):
        print("No ha sido posible establecer conexión con el servidor de registro, inténtelo de nuevo.")
    
    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'client' in locals():
            client.close()
        
# Crear un objeto hash para el algoritmo específico (por ejemplo, SHA-256)
hash_object = hashlib.sha256()

def registry_sockets():
    global ID, token, hash_object
    alias=input("Introduce tu alias: ")
    ADDR = (ad_registry_ip, ad_registry_port)  
     # Solicitar al usuario una contraseña para el dron
    password = input("Introduce tu contraseña: ")

    hash_object.update(password.encode('utf-8'))

    # Obtener el valor hash como una cadena hexadecimal
    hashed_password = hash_object.hexdigest()
    data_to_send = {
        'nombre': alias,
        'contraseña': hashed_password
    }
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")

        print("Realizando solicitud al servidor")
        send(json.dumps(data_to_send), client)
        mensaje = client.recv(2048).decode(FORMAT)
        data = json.loads(mensaje)
        if 'token' in data and 'id' in data:
            token = data['token']
            ID = data['id']
            print(f"Token recibido: {token}")
            print(f"ID recibido: {ID}")
            print(f"Mensaje del servidor: {data.get('mensaje', '')}")
        else:
            print("No se recibió un token en la respuesta.")
            hash_object = hashlib.sha256()

    except (ConnectionRefusedError, TimeoutError):
        print("No ha sido posible establecer conexión con el servidor de registro, inténtelo de nuevo.")
    
    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'client' in locals():
            client.close()

def registry_API():
    global collection, ID, hash_object, token
    try:
        # URL de la API del Registry para agregar un nuevo registro de dron
        api_url = 'http://localhost:5002/registros'  # Ajusta la URL según la configuración de tu servidor
        # Obtener el último documento insertado
        last_document = collection.find_one({}, sort=[("_id", -1)])
        if last_document:
            last_id = last_document["_id"]
            ID = last_id + 1
        else:
            ID += 1

        # Solicitar al usuario un alias para el dron
        alias = input("Introduce tu alias: ")
        # Solicitar al usuario una contraseña para el dron
        password = input("Introduce tu contraseña: ")

        hash_object.update(password.encode('utf-8'))

        # Obtener el valor hash como una cadena hexadecimal
        hashed_password = hash_object.hexdigest()
        # Datos del nuevo registro de dron (puedes ajustar esto según tus necesidades)
        nuevo_registro = {
            '_id': ID,
            'nombre': alias,
            'posicion_x': 0,
            'posicion_y': 0,
            'estado': False,
            'contraseña': hashed_password
        }
        # Realizar la solicitud POST al API del Registry
        response = requests.post(api_url, json=nuevo_registro)
        hash_object = hashlib.sha256()
        # Verificar el código de estado de la respuesta
        if response.status_code == 201:
            data = json.loads(response.text)
            if 'id' in data:
                ID = data['id']
                print(f"Dron ya resgistrado con el ID: {ID}")
            else:
                print(f"Registro agregado correctamente con el ID: {ID}")
            solicitar_nuevo_token_al_servidor()
        else:
            print(f"Error al agregar el registro.")
        if token == "":
            ID = 0
            print("Contraseña incorrecta.\n")
    except Exception as e:
        print(f"Error al realizar la solicitud POST al API del Registry: {str(e)}")

    
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
            for id, datos in mapa.items():
                if datos == (x, y):
                    drones_en_casilla.append(id)
            cantidad_drones = len(drones_en_casilla)
            n = max(n, cantidad_drones)  # Actualiza n con el máximo número de drones encontrados en una casilla
    
    longitud_maxima = max(len(cuadrado),(n))        
    figura_ajustada = {
        int(i): (x - 1, y - 1) for i, (x, y) in figura.items()
    }

    volver_base = {id: (1, 1) for id in figura.keys()}
    # Imprimir línea de números del 1 al 20
    if mapa == figura_ajustada and figura_ajustada != volver_base:
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
            for id, datos in mapa.items():
                if datos == (x, y):
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
    print() 

        
#######   ENGINE   #######

class Producer(threading.Thread):
    global ID, ca_x, ca_y
    ca_x = 0
    ca_y = 0
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.last_sent_message = None  # Almacena el último mensaje enviado a Kafka

    def stop(self):
        self.stop_event.set()

    def run(self):
        global semaforo, estado
        while not self.stop_event.is_set():
            producer = KafkaProducer(bootstrap_servers=self.broker_address)

            position_data = (ID, (ca_x, ca_y), estado)
        
    
            if ca_x == cd_x and ca_y == cd_y:
                if estado == False:
                    print("He llegado a mi destino")
                    estado = True
            else:
                estado = False

            if position_data != self.last_sent_message:
                print(f"Me he movido a ({ca_x+1}, {ca_y+1})")
                semaforo.acquire()
                serialized_position = pickle.dumps(position_data)
                producer.send('topic_a', serialized_position)

                # Almacenar el nuevo mensaje como el último mensaje enviado
                self.last_sent_message = position_data

                # Liberar el permiso del semáforo
                semaforo.release()
            time.sleep(1)
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

    width = 20
    height = 20

    # Calcula las distancias en la dirección x y ajusta para la geometría esférica
    distancia_x = cd_x - ca_x
    if distancia_x > width / 2:
        distancia_x -= width
    elif distancia_x < -width / 2:
        distancia_x += width

    # Calcula las distancias en la dirección y y ajusta para la geometría esférica
    distancia_y = cd_y - ca_y
    if distancia_y > height / 2:
        distancia_y -= height
    elif distancia_y < -height / 2:
        distancia_y += height

    if ca_x == cd_x and ca_y == cd_y:
        return 1
    else:
            # Mueve las datoses de 1 en 1 y ajusta para la geometría esférica
        if distancia_x > 0:
            ca_x += 1
        elif distancia_x < 0:
            ca_x -= 1

        if distancia_y > 0:
            ca_y += 1
        elif distancia_y < 0:
            ca_y -= 1
        # Asegura que las datoses estén dentro de los límites del mapa
        ca_x %= width
        ca_y %= height

class Consumer(threading.Thread):
    global ID, datos_coor
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        try:
            mapa_anterior = None
            figura_anterior = None
            consumer_coord = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            consumer_mapa = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            
            consumer_coord.subscribe(['topic_coord'])
            consumer_mapa.subscribe(['topic_mapa'])

            while not self.stop_event.is_set():
                for coor_message in consumer_coord:

                    datos_coor = coor_message.value
                    for id, datos in datos_coor.items():
                        if ID == int(id):
                            x, y = datos["posicion"]
                            print(f'ID: {id}, Coordenadas:({x}, {y}), Estado: {datos["estado"]}')
                            print()
                            time.sleep(2)

                            semaforo.acquire()
                            setCoords(x-1, y-1)
                            
                            for mapa_message in consumer_mapa:
                                datos_mapa = mapa_message.value
                                mapa_recibido = datos_mapa.get("mapa")
                                figura_recibida = datos_mapa.get("figura")
                                if(figura_anterior == None):
                                    if(mapa_recibido != mapa_anterior):
                                        imprimir_mapa_actualizado(mapa_recibido, figura_recibida)
                                else:
                                    if(mapa_recibido != mapa_anterior):
                                        imprimir_mapa_actualizado(mapa_recibido, figura_anterior)
                                mapa_anterior = mapa_recibido
                                figura_anterior = figura_recibida
                                
                            # Liberar el semáforo después de la actualización del mapa
                            semaforo.release()
                if self.stop_event.is_set():
                    break

        except Exception as e:
            print(f"Error en el consumidor: {e}")

########## MAIN ##########

def main(argv = sys.argv):
    if len(sys.argv) != 7:
        print("Error: El formato debe ser el siguiente: [IP_Engine] [Puerto_Engine] [IP_Broker] [Puerto_Broker] [IP_Registry] [Puerto_Registry]")
        sys.exit(1)
    else:  
        global token, ad_engine_ip, ad_engine_port, broker_ip, broker_port, ad_registry_ip, ad_registry_port, ID
        
        ad_engine_ip = sys.argv[1]
        ad_engine_port = int(sys.argv[2])
        broker_ip = sys.argv[3]
        broker_port = int(sys.argv[4])
        ad_registry_ip = sys.argv[5]
        ad_registry_port = int(sys.argv[6])
        orden = ""
        while(orden != "4"):
            
            print("\n¿Qué deseas hacer?")
            print("1. Registrarse con Sockets")
            print("2. Registrarse con API")
            print("3. Empezar representación")
            print("4. Apagarse")
            print("Opción:", end=" ")
            orden = input()
            print()
            
            while orden != "1" and orden != "2" and orden != "3" and orden != "4":
                print("Error, indica una de las 4 posibilidades por favor(1, 2, 3 o 4).")
                print("Opción:", end=" ")
                orden = input()
                print()

            if orden == "1":
                if(ID != 0):
                    print("Ya estás registrado!")
                    print()
                else:
                    registry_sockets()

            if orden == "2":
                if(ID != 0):
                    print("Ya estás registrado!")
                else:
                    registry_API()

            if orden == "3":
                print(token)
                if(ID == 0):
                    print("No estás registrado!")
                    print()
                elif(token != "" and verificar_tiempo_de_expiracion(token) == False):
                    tasks = [Consumer(), Producer()]

                    for t in tasks:
                        t.start()

                    while True:
                        time.sleep(1)
                else:
                    print("El token ha expirado. Solicitando nuevo token...")
                    solicitar_nuevo_token_al_servidor()
                    #print(token)

            if orden == "4":
                sys.exit()

if __name__ == "__main__":
  main(sys.argv[1:])

 # python3 AD_Drone.py 127.0.0.1 5050 127.0.0.1 29092 127.0.0.1 5051
