# API_Engine.py

from flask import Flask, jsonify, abort
from flask_cors import CORS
from pymongo import MongoClient, server_api

app = Flask(__name__)
CORS(app) 

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


@app.route('/estado_mapa', methods=['GET'])
def estado_mapa():
    try:
         # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Recuperar todos los drones de la colección
        drones = collection.find()

        # Crear un diccionario con la información de posición y alias de los drones
        estado_mapa = {str(dron['_id']): {'alias': dron['nombre'], 'posicion_x': dron['posicion_x'], 'posicion_y': dron['posicion_y']} for dron in drones}
        

        print("Estado del mapa:", estado_mapa)
        return jsonify(estado_mapa)
    except Exception as e:
        abort(500, f"Error al obtener el estado del mapa: {str(e)}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)