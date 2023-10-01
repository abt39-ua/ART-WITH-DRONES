import socket 
import threading
import os
import signal
import sys

nom_archivo = "registro.txt"
ID = 1
NEXT_ID = 1

HEADER = 64
PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando el servidor...")
    # Borrar el archivo registro.txt al cerrar el servidor
    try:
        if os.path.exists(nom_archivo):
            os.remove(nom_archivo)
            print(f"Archivo {nom_archivo} eliminado al cerrar el servidor.")
        else:
            print(f"El archivo {nom_archivo} no existe.")
    except Exception as e:
        print(f"No se pudo eliminar el archivo {nom_archivo}: {str(e)}")
    sys.exit(0)  # Sale del programa
    server.close()  # Cierra el socket del servidor

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)


def handle_client(conn, addr):
    global ID # hacemos referencia a la variable global ID
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
            conn.send(f"Se te ha asignado el ID: {ID}, y el alias {msg}".encode(FORMAT))
            save_info(ID, msg)
            ID += 1  # Incrementar el ID para el próximo cliente
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    

def start():
    global server
    try:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Habilita la opción SO_REUSEADDR
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
    except Exception as e:
        print(f"Error al iniciar el servidor: {str(e)}")

def save_info(ID, alias):
# Se comprueba que el archivo está creado y si no lo esta, lo crea
    try:
        with open(nom_archivo, 'a') as registro:
            registro.write(f"ID: {ID}, Alias: {alias}\n")
        print("Información guardada con éxito.")
    except Exception as e:
        print(f"Error al guardar la información: {str(e)}")


######################### MAIN ##########################


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")

start()
