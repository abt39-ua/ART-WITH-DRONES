#SAMUEL LORENZO
#ALICIA BAQUERO
import socket 
import threading
import os
import signal
import sys
import hashlib
import json
import jwt
from datetime import datetime, timedelta
from pymongo import MongoClient, server_api, errors
from flask import Flask, jsonify, request
app = Flask(__name__)

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

# Función para generar un token JWT
def generate_jwt_token(dron_id, expiration_seconds=20):
    try:
        # Calcular la marca de tiempo de expiración
        expiration_time = datetime.utcnow() + timedelta(seconds=expiration_seconds)

        # Construir el payload del token
        payload = {
            'dron_id': dron_id,
            'exp': expiration_time
        }

        # Reemplaza 'tu_clave_secreta' con tu clave secreta real y segura
        secret_key = '123'

        # Generar el token
        token = jwt.encode(payload, secret_key, algorithm='HS256')

        return token
    except Exception as e:
        print(f"Error al generar el token: {e}")
        return None

# VÍA SOCKETS

def handle_client_socket(conn, addr):
    global ID, collection
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if "{" in msg and "}" in msg:
                # Si msg parece ser un JSON, intenta cargarlo
                try:
                    data = json.loads(msg)
                    if "nombre" in data and "contraseña" in data:
                        alias = data["nombre"]
                        hashed_password_from_client = data["contraseña"]
                          
                        n = buscar_alias(alias)
                        
                        print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
                        print()
                        print(n)
                        if n == "0":
                            # Obtener el último documento insertado
                            last_document = collection.find_one({}, sort=[("_id", -1)])

                            if last_document:
                                last_id = last_document["_id"]
                                ID = last_id + 1
                
                            token = generate_jwt_token(ID)
                            # Enviar el token y el ID junto con la respuesta al dron
                            response_data = {'token': token, 'id': ID, 'mensaje': 'Autenticación exitosa'}
                            with open("auditoria.txt", "a") as file:
                                file.write(f"Un dron se ha registrado con el alias: {alias} y se le ha asignado el ID: {ID}.\n")
                            #token_str = token.decode('UTF-8')
                            # Enviar el token y el ID junto con la respuesta al dron
                            #response_data = {'token': token_str, 'id': ID, 'mensaje': 'Autenticación exitosa'}
                            response = json.dumps(response_data)
                            conn.send(response.encode(FORMAT))
                            save_info(ID, msg)
                            ID += 1
                            
                            
                        else:
                            correcta = comprobar_contraseñas(alias, hashed_password_from_client)
                            try: 
                                if(correcta):
                                    # Contraseña válida, enviamos el nuevo token y el ID junto con la respuesta al dron
                                    nuevo_token = generate_jwt_token(n)
                                    with open("auditoria.txt", "a") as file:
                                        ID = buscar_alias(alias)
                                        file.write(f"El dron con el alias: {alias} e ID: {ID}, se ha levantado correctamente.\n")
                                    # Enviar el nuevo token y el ID junto con la respuesta al dron
                                    response_data = {'token': nuevo_token, 'id': n, 'mensaje': f'Este nombre ya está registrado con el ID: {n}'}
                                else:
                                    with open("auditoria.txt", "a") as file:
                                        file.write(f"Un dron ha intentado autenticarse con el alias: {alias}, con una contraseña incorrecta.\n")
                                    # Contraseña incorrecta, enviamos un mensaje de error al dron
                                    response_data = {'mensaje': 'Contraseña incorrecta'}
                                
                                response = json.dumps(response_data)
                                conn.send(response.encode(FORMAT))
                            except Exception as e:
                                print(f"Error al construir la respuesta JSON: {e}") 
                except json.JSONDecodeError:
                    # No es un JSON válido
                    print("Mensaje no válido. Se espera un JSON.")
            else:
                # Si msg es un int, es un ID
                if msg.isdigit():
                    token = generate_jwt_token(int(msg))
                    response_data = {'token': token}
                    response = json.dumps(response_data)
                    conn.send(response.encode(FORMAT))
                else:
                    print("Mensaje en formato incorrecto.")      
    conn.close()

#VIA API_REST
@app.route('/registros', methods=['GET'])
def obtener_registros():
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Recuperar todos los registros de drones de la colección
        registros = list(collection.find())

        return jsonify(registros)

    except Exception as e:
        return jsonify({"mensaje": f"Error al obtener los registros de drones: {str(e)}"}), 500

# Ruta para agregar un nuevo registro de dron
@app.route('/registros', methods=['POST'])
def agregar_registro():
    try:

        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Obtener el JSON del cuerpo de la solicitud
        nuevo_registro = request.json

        if "nombre" in nuevo_registro and "contraseña" in nuevo_registro:
            alias = nuevo_registro["nombre"]
            hashed_password_from_client = nuevo_registro["contraseña"]
            print(alias)
            print(hashed_password_from_client)
            n = buscar_alias(alias)
            
            if n == "0":
                # Insertar el nuevo registro en la base de datos
                result = collection.insert_one(nuevo_registro)
                token = generate_jwt_token(ID)
                if result.inserted_id:
                    return jsonify({"mensaje": f"Registro agregado correctamente con ID: {result.inserted_id}"}), 201
            else:
                correcta = comprobar_contraseñas(alias, hashed_password_from_client)
                print(correcta)
                if correcta:
                    with open("auditoria.txt", "a") as file:
                        file.write(f"El dron con el alias: {alias} e ID: {n}, se ha levantado correctamente.\n")
                    print(n)
                    # Contraseña válida, enviamos el nuevo token y el ID junto con la respuesta al dron
                    nuevo_token = generate_jwt_token(n)
                    response_data = {'token': nuevo_token, 'id': n, 'mensaje': f'Este nombre ya está registrado con el ID: {n}'}
                else:
                    with open("auditoria.txt", "a") as file:
                        file.write(f"Un dron ha intentado autenticarse con el alias: {alias}, con una contraseña incorrecta.\n")
                    # Contraseña incorrecta, enviamos un mensaje de error al dron
                    response_data = {'mensaje': 'Contraseña incorrecta'}

                return jsonify(response_data), 201
        else:
            return jsonify({"mensaje": "Error, información a almacenar incorrecta."}), 400

    except Exception as e:
        return jsonify({"mensaje": f"Error al agregar el registro: {str(e)}"}), 500

# Ruta para modificar un registro de dron
@app.route('/registros/<string:registro_id>', methods=['PUT'])
def modificar_registro(registro_id):
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Obtener el JSON del cuerpo de la solicitud
        nuevo_estado = request.json

        # Modificar el registro en la base de datos
        result = collection.update_one({'_id': registro_id}, {'$set': nuevo_estado})

        if result.modified_count > 0:
            return jsonify({"mensaje": f"Registro con ID {registro_id} modificado correctamente"})
        else:
            return jsonify({"mensaje": f"No se encontró el registro con ID {registro_id} para modificar"}), 404

    except Exception as e:
        return jsonify({"mensaje": f"Error al modificar el registro: {str(e)}"}), 500

# Ruta para eliminar un registro de dron
@app.route('/registros/<string:registro_id>', methods=['DELETE'])
def eliminar_registro(registro_id):
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Eliminar el registro de la base de datos
        result = collection.delete_one({'_id': registro_id})

        if result.deleted_count > 0:
            return jsonify({"mensaje": f"Registro con ID {registro_id} eliminado correctamente"})
        else:
            return jsonify({"mensaje": f"No se encontró el registro con ID {registro_id} para eliminar"}), 404

    except Exception as e:
        return jsonify({"mensaje": f"Error al eliminar el registro: {str(e)}"}), 500

def run_api_server():
    app.run(host='0.0.0.0', port=5002)

def comprobar_contraseñas(alias, hashed_password_from_client):
    global collection
    print("Comprobando contraseñas:")
    # Dron ya registrado, verificamos la contraseña
    stored_document = collection.find_one({"nombre": alias})
    print("Imprimiendo info:")
    print(stored_document)
    print()
    stored_password = stored_document.get("contraseña", "")
    print("Contraseña de la BD:")
    print(stored_password)
    print("Contraseña del cliente:")
    print(hashed_password_from_client)
    print()
    try:
        if (stored_password == hashed_password_from_client):
            return True
        else:
            return False
    except Exception as e:
        print(f"Error al verificar la contraseña: {e}")

def handle_socket_connections():
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
                thread = threading.Thread(target=handle_client_socket, args=(conn, addr))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO:", MAX_CONEXIONES-CONEX_ACTIVAS)
            else:
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    except Exception as e:
        print(f"Error al iniciar el servidor: {str(e)}")

def save_info(ID, msg):
    try:
        collection = conectar_db()
        print("Conexión exitosa a la base de datos")
        data = json.loads(msg)
        if "nombre" in data and "contraseña" in data:
            alias = data["nombre"]
            hashed_password_from_client = data["contraseña"]          
            existing_document = collection.find_one({"nombre": alias})
            if existing_document:
                print(f"Error al insertar la información en la base de datos: El alias '{alias}' ya existe.")
            else:
                # Se realiza la inserción en la colección "drones" con el ID especificado
                collection.insert_one({"_id": ID, "nombre": alias,"contraseña": hashed_password_from_client, "posicion_x": 0, "posicion_y": 0, "estado":False})
                print("Información insertada con éxito en la base de datos.")
                print()
                consultar_info()
        else:
            print("Error, información a almacenar incorrecta.")
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


if __name__ == "__main__":
    main(sys.argv[1:])

    # Iniciar servidor de sockets en un hilo separado
    socket_thread = threading.Thread(target=handle_socket_connections)
    socket_thread.start()

    # Iniciar servidor API en otro hilo separado
    api_thread = threading.Thread(target=run_api_server)
    api_thread.start()

    # Esperar a que ambos hilos finalicen (si es necesario)
    socket_thread.join()
    api_thread.join()