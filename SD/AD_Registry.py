import socket 
import threading
import os
import sys

nom_archivo = "registro.txt"

HEADER = 64
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


def handle_client(conn, addr):
    global ID
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            Alias = buscar_alias(msg)
            print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
            if Alias == "0":
                conn.send(f"{ID}".encode(FORMAT))
                save_info(ID, msg)
                ID = ID + 1

            else:
                conn.send(f"Este Dron ya estaba registrado con el ID: {ID}".encode(FORMAT))


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


def save_info(ID, alias):
    # Se comprueba que el archivo está creado y si no lo esta, lo crea
    try:
        with open(nom_archivo, 'a') as registro:
            registro.write(f"ID: {ID}, Alias: {alias}\n")
        print("Información guardada con éxito.")
    except Exception as e:
        print(f"Error al guardar la información: {str(e)}")


######################### MAIN ##########################

def main(argv = sys.argv):
    global server, SERVER, ID

    PORT = int(sys.argv[1])
    SERVER = sys.argv[2]
    ADDR = (SERVER, PORT)
    opcion = int(sys.argv[3])

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)

    print("[STARTING] Servidor inicializándose...")

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