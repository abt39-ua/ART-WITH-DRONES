import socket 
import threading
import os
import signal
import sys

nom_archivo = "registro.txt"
ID = 1
NEXT_ID = 1

HEADER = 64
ad_registry_port = 0
ad_registry_ip = 0
ADDR = 0
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 100
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def borrar_archivo():
    try:
        if os.path.exists(nom_archivo):
            os.remove(nom_archivo)
            print(f"Archivo {nom_archivo} eliminado al cerrar el servidor.")
        else:
            print(f"El archivo {nom_archivo} no existe.")
    except Exception as e:
        print(f"No se pudo eliminar el archivo {nom_archivo}: {str(e)}")

def signal_handler(sig, frame):
    global server
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando el servidor...")
    # Borrar el archivo registro.txt al cerrar el servidor
    borrar_archivo()
    server.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

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
            conn.send(str(ID).encode(FORMAT))
            save_info(ID, msg)
            ID += 1  # Incrementar el ID para el próximo cliente
    conn.close()


def start():
    global server, ad_registry_ip
    try:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Habilita la opción SO_REUSEADDR
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {ad_registry_ip}")
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
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    except Exception as e:
        print(f"Error al iniciar el servidor: {str(e)}")

def save_info(ID, alias):
# Se comprueba que el archivo está creado y si no lo esta, lo crea
    try:
        with open(nom_archivo, 'a') as registro:
            registro.write(f"{ID},{alias}\n")
        print("Información guardada con éxito.")
    except Exception as e:
        print(f"Error al guardar la información: {str(e)}")


######################### MAIN ##########################


def main(argv = sys.argv):
    global ad_registry_port, ADDR, ad_registry_ip, server
    if len(sys.argv) != 2:
        print("Error: El formato debe ser el siguiente: [Puerto_escucha]")
        sys.exit(1)
    else:  
        global ad_engine_ip, ad_engine_port, broker_ip, broker_port, ad_registry_ip, ad_registry_port
        
        ad_registry_port = int(sys.argv[1])
        ad_registry_ip = socket.gethostbyname(socket.gethostname())
        ADDR = (ad_registry_ip, ad_registry_port)
        server.bind(ADDR)

        print("[STARTING] Servidor inicializándose...")

        start()
if __name__ == "__main__":
  main(sys.argv[1:])



