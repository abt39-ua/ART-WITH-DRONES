import socket
import argparse

HEADER = 64
PORT = 5050
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
            
            
            msg_invertida = msg[::-1]
            
            conn.send(f"HOLA CLIENTE: Aquí tienes tu mensaje invertido: {msg_invertida}".encode(FORMAT))
            print(f"Cadena invertida: {msg_invertida}")
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()

def start():
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

# Función que obtiene el nombre de la ciudad a partir de su nombre en minúscula.
# Si la ciudad no se encuentra en el diccionario, devuelve "Ciudad no encontrada".

def obtener_nombre_ciudad(ciudad, datos_ciudades):
    ciudad = ciudad.lower()
    if ciudad in datos_ciudades:
        return ciudad, datos_ciudades[ciudad]
    else:
        return "Ciudad no encontrada", None

def main():
    try:
        # Abrimos el archivo en modo lectura y lo asignamos a la variable "archivo".
        # Se cerrará automáticamente cuando salgamos del ámbito.
        with open('ciudades.txt', 'r') as archivo: 
            # Inicializamos un diccionario para almacenar los datos de las ciudades
            datos_ciudades = {}
            
            # Leemos cada línea del archivo y cargamos los datos en el diccionario
            for linea in archivo:
                # Divide la línea en dos partes usando ':' como separador
                ciudad, valor_str = linea.strip().split(':')
                
                # Convertimos el valor a un entero
                valor = int(valor_str)
                
                # Almacenamos la información en el diccionario
                datos_ciudades[ciudad] = valor
        
        # Configuración del servidor
        parser = argparse.ArgumentParser(description="Servidor de Ciudad")
        parser.add_argument("puerto", type=int, help="Puerto de escucha")
        args = parser.parse_args()

        host = "127.0.0.1"  # Dirección IP del servidor

        # Crear un socket TCP/IP
        servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Enlazamos el socket al host y puerto especificados
        servidor_socket.bind((host, args.puerto))

        # Escuchamos conexiones entrantes (máximo 5 conexiones en espera)
        servidor_socket.listen(5)

        print(f"Servidor escuchando en {host}:{args.puerto}")

        while True:
            try:
                # Aceptamos una conexión entrante
                cliente_socket, cliente_direccion = servidor_socket.accept()
                print(f"Conexión entrante de {cliente_direccion}")

                # Recibimos datos del cliente y pasarlos al formato del fichero (minúscula)
                datos = cliente_socket.recv(1024).decode("utf-8").strip().lower()

                if datos:
                    print(f"Peticion recibida: {datos}")
                    # Definimos ciudades válidas basadas en las claves del diccionario
                    ciudades_validas = datos_ciudades.keys()
                    # Verificamos si la ciudad solicitada es válida
                    ciudad, valor = obtener_nombre_ciudad(datos, datos_ciudades)
                    if ciudad != "Ciudad no encontrada" and ciudad in ciudades_validas and valor is not None:
                        if valor > 0:
                            respuesta = f"En la ciudad: {ciudad}, se puede actuar, ya que hacen {valor} grados." 
                        else:
                            respuesta = f"En la ciudad: {ciudad}, NO se puede actuar, ya que hacen {valor} grados." 
                    else:
                        respuesta = "Ciudad no válida"
                else:
                    respuesta = "Peticion vacia"

                # Enviar la respuesta al cliente
                cliente_socket.send(respuesta.encode("utf-8"))

                # Cerrar la conexión con el cliente
                cliente_socket.close()

            # Si el usuario detiene manualmente el servidor
            except KeyboardInterrupt:
                print("Servidor detenido por el usuario.")
                break  # Salir del bucle en caso de interrupción manual

    # Si no se encuentra el fichero "ciudades.txt"
    except FileNotFoundError:
        print("Error: El archivo 'ciudades.txt' no se encuentra.")
    
    # Si los grados asignados a una ciudad no pueden convertirse en entero
    except ValueError:
        print("Error: No se pudo convertir un valor a entero.")

    # Si hay problemas en la creación o enlace del socket
    except socket.error as e:
        print(f"Error en el socket: {e}")
    
    # Cuando se produzca algún error sin especificar anteriormente
    except Exception as e:
        print(f"Error desconocido: {e}")
    finally:
        servidor_socket.close()

