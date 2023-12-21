#SAMUEL LORENZO
#ALICIA BAQUERO
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
        base = (0,0)
        # Crear un diccionario con la información de posición y alias de los drones
        estado_mapa = {str(dron['_id']): {'alias': dron['nombre'], 'posicion_x': dron['posicion_x'], 'posicion_y': dron['posicion_y'], 'estado': dron['estado']} for dron in drones}
        print("Estado del mapa:", estado_mapa)
        # Verificar si todos los drones están en la base (posición (1, 1))
        todos_en_base = all((info_dron['posicion_x'], info_dron['posicion_y']) == base for info_dron in estado_mapa.values())
        if todos_en_base:
            for dron_id, info_dron in estado_mapa.items():
                estado_mapa[dron_id]['estado'] = False
        
        return jsonify(estado_mapa)
    except Exception as e:
        abort(500, f"Error al obtener el estado del mapa: {str(e)}")

@app.route('/drones', methods=['POST'])
def agregar_dron():
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Obtener el JSON del cuerpo de la solicitud
        nuevo_dron = request.json

        # Insertar el nuevo dron en la base de datos
        result = collection.insert_one(nuevo_dron)

        if result.inserted_id:
            return jsonify({"mensaje": f"Dron agregado correctamente con ID: {result.inserted_id}"}), 201
        else:
            return jsonify({"mensaje": "Error al agregar el dron"}), 500

    except Exception as e:
        abort(500, f"Error al agregar el dron: {str(e)}")

@app.route('/drones/<string:dron_id>', methods=['PUT'])
def modificar_dron(dron_id):
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Obtener el JSON del cuerpo de la solicitud
        nuevo_estado = request.json

        # Modificar el dron en la base de datos
        result = collection.update_one({'_id': int(dron_id)}, {'$set': nuevo_estado})

        if result.modified_count > 0:
            return jsonify({"mensaje": f"Dron con ID {dron_id} modificado correctamente"})
        else:
            return jsonify({"mensaje": f"No se encontró el dron con ID {dron_id} para modificar"})

    except Exception as e:
        abort(500, f"Error al modificar el dron: {str(e)}")

@app.route('/drones/<string:dron_id>', methods=['DELETE'])
def eliminar_dron(dron_id):
    try:
        # Obtener la colección desde la conexión a la base de datos
        collection = conectar_db()

        # Eliminar el dron de la base de datos
        result = collection.delete_one({'_id': int(dron_id)})

        if result.deleted_count > 0:
            return jsonify({"mensaje": f"Dron con ID {dron_id} eliminado correctamente"})
        else:
            return jsonify({"mensaje": f"No se encontró el dron con ID {dron_id} para eliminar"})

    except Exception as e:
        abort(500, f"Error al eliminar el dron: {str(e)}")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)