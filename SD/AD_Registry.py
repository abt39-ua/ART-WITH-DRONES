import socket 
import threading
import os
import signal
import sys
from pymongo import MongoClient, server_api, errors

ID = 1
HEADER = 64
ADDR = 0
ad_registry_port = 0
ad_registry_ip = 0
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 100
opcion_borrar = -1
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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

def buscar_alias(alias):
    try:
        collection = conectar_db()
        # Buscar si el alias ya existe en la base de datos
        document = collection.find_one({"nombre": alias})
        print(document)
        if document:
            return document["_id"]
        else:
            return "0"
    except Exception as e:
        print(f"Error al buscar alias en la base de datos: {str(e)}")
        return "0"

def signal_handler(sig, frame):
    global server
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando el servidor...")
    server.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)


def handle_client(conn, addr):
    global ID
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            n = buscar_alias(msg)
            print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
            print()
            print(n)
            if n == "0":
                conn.send(f"{ID}".encode(FORMAT))
                save_info(ID, msg)
                ID += 1

            else:
                conn.send(f"Este Dron ya estaba registrado con el ID: {n}".encode(FORMAT))

    conn.close()


def start():
    global server, ad_registry_ip
    try:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Habilita la opción SO_REUSEADDR
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {ad_registry_ip}")
        print()
        CONEX_ACTIVAS = threading.active_count()-1
        while True:
            conn, addr = server.accept()
            CONEX_ACTIVAS = threading.active_count()
            if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO:", MAX_CONEXIONES-CONEX_ACTIVAS)
            else:
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    except Exception as e:
        print(f"Error al iniciar el servidor: {str(e)}")

def save_info(ID, alias):
    try:
        collection = conectar_db()
        print("Conexión exitosa a la base de datos")

        existing_document = collection.find_one({"nombre": alias})
        if existing_document:
            print(f"Error al insertar la información en la base de datos: El alias '{alias}' ya existe.")
        else:
            # Se realiza la inserción en la colección "drones" con el ID especificado
            collection.insert_one({"_id": ID, "nombre": alias, "posicion_x": 0, "posicion_y": 0, "estado":False})
            print("Información insertada con éxito en la base de datos.")
            print()
            consultar_info()

    except errors.DuplicateKeyError as e:
        print(f"Error al insertar la información en la base de datos: {_id} ya existe.")
    
    except Exception as e:
        print(f"Error al insertar la información en la base de datos: {str(e)}")

def consultar_info():
    try:
        collection = conectar_db()
        print("Conexión exitosa a la base de datos")

        # Consultar todos los documentos en la colección "drones"
        cursor = collection.find()

        # Imprimir los documentos
        for document in cursor:
            print(document)

    except Exception as e:
        print(f"Error al consultar la información en la base de datos: {str(e)}")


def borrar_registros_db():
    try:
        collection = conectar_db()
        print("Conexión exitosa a la base de datos")

        # Elimina todos los registros de la colección "drones"
        result = collection.delete_many({})

        print(f"{result.deleted_count} registros en la base de datos eliminados.")
        consultar_info()
    except Exception as e:
        print(f"Error al borrar registros de la base de datos: {str(e)}")


######################### MAIN ##########################

def main(argv = sys.argv):
    global ad_registry_port, ad_registry_ip, server, ADDR, opcion_borrar, ID
    if len(sys.argv) != 4:
        print("Error: El formato debe ser el siguiente: [Puerto_escucha] [IP_Registry] [Opcion_Borrar]")
        sys.exit(1)
    else:  
        ad_registry_port = int(sys.argv[1])
        ad_registry_ip = sys.argv[2]
        ADDR = (ad_registry_ip, ad_registry_port)
        server.bind(ADDR)

        # Verificar la opción de borrar archivo
        opcion_borrar = int(sys.argv[3])
        if opcion_borrar == 0:
            borrar_registros_db()
            print("Registros en la base de datos eliminados.")
        elif opcion_borrar == 1:
            collection = conectar_db()
            last_document = collection.find_one({}, sort=[("_id", -1)])  # Obtener el último documento insertado
            if last_document:
                last_id = last_document["_id"]
                ID = last_id + 1  # Establecer ID al siguiente ID disponible

            # Conservar el registro anterior (no borrar archivo)
            pass
        else: 
            print("Opción borrar incorrecta, debe ser 1 ó 0.")
            sys.exit(1)
        print("[STARTING] Servidor inicializándose...")

        start()

if __name__ == "__main__":
  main(sys.argv[1:])