import socket
import argparse
import threading
import sys

HEADER = 64
SERVER = socket.gethostbyname(socket.gethostname())

FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2
DATOS_CIUDADES = {}

# Función que obtiene el nombre de la ciudad a partir de su nombre en minúscula.
# Si la ciudad no se encuentra en el diccionario, devuelve "Ciudad no encontrada".

def obtener_nombre_ciudad(ciudad, DATOS_CIUDADES):
    ciudad = ciudad.lower()
    if ciudad in DATOS_CIUDADES:
        return ciudad, DATOS_CIUDADES[ciudad]
    else:
        return "Ciudad no encontrada", None

def procesar_archivo():
    try:
        # Abrimos el archivo en modo lectura y lo asignamos a la variable "archivo".
        # Se cerrará automáticamente cuando salgamos del ámbito.
        with open('ciudades.txt', 'r') as archivo: 
            
            # Leemos cada línea del archivo y cargamos los datos en el diccionario
            for linea in archivo:
                # Divide la línea en dos partes usando ':' como separador
                ciudad, valor_str = linea.strip().split(':')
                
                # Convertimos el valor a un entero
                valor = int(valor_str)
                
                # Almacenamos la información en el diccionario
                DATOS_CIUDADES[ciudad] = valor
    
    # Si no se encuentra el fichero "ciudades.txt"
    except FileNotFoundError:
        print("Error: El archivo 'ciudades.txt' no se encuentra.")

def handle_client(conn, addr):
    try:
        print(f"[NUEVA CONEXIÓN] {addr} se ha conectado.")  # Muestra un mensaje cuando un cliente se conecta

        connected = True
        while connected:
            msg_length = conn.recv(HEADER).decode(FORMAT)  # Recibe la longitud del mensaje
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)  # Recibe el mensaje completo
                if msg == FIN:  # Si el cliente envía "FIN", se desconecta
                    connected = False
                print(f"El cliente [{addr}] solicita información sobre la ciudad: {msg}")

                # Verificamos si la ciudad solicitada es válida
                ciudad, valor = obtener_nombre_ciudad(msg, DATOS_CIUDADES)
                # Definimos ciudades válidas basadas en las claves del diccionario
                ciudades_validas = DATOS_CIUDADES.keys()
                if ciudad != "Ciudad no encontrada" and ciudad in ciudades_validas and valor is not None:
                    if valor > 0:
                        conn.send(f"En la ciudad {ciudad}, se puede actuar, ya que hacen {valor} grados.".encode(FORMAT)) 
                    else:
                        conn.send(f"En la ciudad {ciudad}, NO se puede actuar, ya que hacen {valor} grados.".encode(FORMAT)) 
                else:
                    respuesta = "Ciudad no válida"

        print("ADIOS. Cliente [{addr}] desconectado.") # Mensaje de despedida al cliente
        conn.close()  # Cierra la conexión con el cliente
    except ConnectionResetError:
        print(f"Error de conexión con el cliente [{addr}].")
    except socket.error as e:
        print(f"Error en la comunicación con el cliente [{addr}]: {e}")
    except ValueError:
        print(f"Error: No se pudo convertir un valor a entero.")
    except Exception as e:
        print(f"Error desconocido con el cliente [{addr}]: {e}")
    

# Función principal para iniciar el servidor
def main(argv = sys.argv):
    global ADDR
    PORT = int(argv[0])
    ADDR = (SERVER, PORT)

    # Crear un socket del servidor y enlazarlo al puerto y dirección.
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Crea un socket de tipo TCP/IP
    server.bind(ADDR)  # Enlaza el socket a la dirección y puerto especificados
    procesar_archivo()

    try:
        server.listen()  # Empieza a escuchar por conexiones entrantes
        print(f"[ESCUCHANDO] Servidor a la escucha en {SERVER}")
        
        # Obtiene el número de conexiones activas (hilos de clientes)
        CONEX_ACTIVAS = threading.active_count() - 1
        print(CONEX_ACTIVAS)

        while True:
            conn, addr = server.accept()  # Acepta una nueva conexión entrante
            CONEX_ACTIVAS = threading.active_count()  # Actualiza el número de conexiones activas
            
            if CONEX_ACTIVAS <= MAX_CONEXIONES: 
                # Si no se ha alcanzado el límite de conexiones activas, crea un nuevo hilo para manejar al cliente
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()  # Inicia el hilo para el cliente
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES - CONEX_ACTIVAS)
            else:
                # Si se ha alcanzado el límite, rechaza la conexión y envía un mensaje al cliente
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count() - 1
    # 
    except KeyboardInterrupt:
        print("Servidor detenido por el usuario.")

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
        server.close()




if __name__ == "__main__":
  main(sys.argv[1:])