import socket 
import threading
import os
import sys

nom_archivo = "registro.txt"
ID = 1

HEADER = 64
ADDR = 0
ad_registry_port = 0
ad_registry_ip = 0
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 20
ID = 1


def buscar_alias(alias):
    with open("registro.txt", 'r') as file:
        for line in file:
            if f"Alias: {alias}" in line:
                parts = line.split(", ")
                for part in parts:
                    if part.startswith("ID: "):
                        return int(part.split(":")[1].strip())
                break
    return "0"

def signal_handler(sig, frame):
    global server
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando el servidor...")
    server.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)
def buscar_alias(alias):
    with open("registro.txt", 'r') as file:
        for line in file:
            if f"Alias: {alias}" in line:
                parts = line.split(", ")
                for part in parts:
                    if part.startswith("ID: "):
                        return int(part.split(":")[1].strip())
                break
    return "0"


def handle_client(conn, addr):
    global ID
    global ID
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            n = buscar_alias(msg)
            Alias = buscar_alias(msg)
            print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
            print()
            if n == "0":
                conn.send(f"{ID}".encode(FORMAT))
                save_info(ID, msg)
                ID = ID + 1

            else:
                conn.send(f"Este Dron ya estaba registrado con el ID: {n}".encode(FORMAT))

            if Alias == "0":
                conn.send(f"{ID}".encode(FORMAT))
                save_info(ID, msg)
                ID = ID + 1

            else:
                conn.send(f"Este Dron ya estaba registrado con el ID: {ID}".encode(FORMAT))


    print("ADIOS. TE ESPERO EN OTRA OCASION")
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


def save_info(ID, alias):
    # Se comprueba que el archivo está creado y si no lo esta, lo crea
    # Se comprueba que el archivo está creado y si no lo esta, lo crea
    try:
        with open(nom_archivo, 'a') as registro:
            registro.write(f"ID: {ID}, Alias: {alias}\n")
            registro.write(f"ID: {ID}, Alias: {alias}\n")
        print("Información guardada con éxito.")
        print()
    except Exception as e:
        print(f"Error al guardar la información: {str(e)}")

######################### MAIN ##########################

def main(argv = sys.argv):
    global ad_registry_port, ad_registry_ip, server, ADDR
    if len(sys.argv) != 4:
        print("Error: El formato debe ser el siguiente: [Puerto_escucha] [IP_Registry] [Opcion_Borrar]")
        sys.exit(1)
    else:  
        ad_registry_port = int(sys.argv[1])
        ad_registry_ip = sys.argv[2]
        ADDR = (ad_registry_ip, ad_registry_port)
        server.bind(ADDR)
    global server, SERVER, ID

    PORT = int(sys.argv[1])
    SERVER = sys.argv[2]
    ADDR = (SERVER, PORT)
    opcion = int(sys.argv[3])

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

        # Verificar la opción de borrar archivo
        opcion_borrar = int(sys.argv[3])
        if opcion_borrar == 0:
            with open(nom_archivo, 'w') as registro:
                pass
        elif opcion_borrar == 1:
            # Conservar el registro anterior (no borrar archivo)
            pass
        else: 
            print("Opción borrar incorrecta, debe ser 1 ó 0.")
            sys.exit(1)
        print("[STARTING] Servidor inicializándose...")
    print("[STARTING] Servidor inicializándose...")

        start()

    #Vaciamos el registro anterior si 1, para continuar 0
    modo = 'a' if opcion == 0 else 'w'
    with open(nom_archivo, modo) as registro:
        if modo == 1:
            registro.truncate(0)

    with open(nom_archivo, 'r') as file: 
        lineas = file.readlines()
        numlin = len(lineas)
        ID = numlin + 1

    start()

if __name__ == "__main__":
    main(sys.argv[1:])

# python3 AD_Registry.py 5051 172.20.53.43 0
#0 para continuar, 1 para reiniciar