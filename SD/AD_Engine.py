import socket
import sys
import time
import signal
import random

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ciudades = {}

def obtener_nombre_ciudades():
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

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando conexión...")
    client.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)

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

    
########## MAIN ##########8
def start():
    
    if  (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (SERVER, PORT)
        try:
            tiempo_de_consulta = int(sys.argv[3])
        except ValueError:
            print("El tiempo de consulta debe ser un número entero.")
            sys.exit()
        
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")
        obtener_nombre_ciudades()
        contador = 0
        ciudad_random = random.choice(list(ciudades.keys()))
        msg = ciudad_random
        while contador < 10:
            print("Envio al servidor: ", msg)
            send(msg)
            print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
            # Pausar el programa durante el tiempo de consulta antes de enviar la próxima consulta
            time.sleep(tiempo_de_consulta)
            contador += 1

        client.close()

    else:
        print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Ciudad> <Tiempo consulta>")


start()
# Crear una matriz 2D de 20x20 posiciones para representar el espacio aéreo
# espacio_aereo = [[0 for _ in range(20)] for _ in range(20)]

# Imprimir la matriz para visualizar el espacio aéreo
# for fila in espacio_aereo:
    # print(fila)