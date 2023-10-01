import socket 
import threading
import os

nom_archivo = "registro.txt"

HEADER = 64
PORT = 5051
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2



def handle_client(conn, addr, ID):
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            else:
                print(f"He recibido del cliente [{addr}] el mensaje: {msg}")
                conn.send(f"{ID}".encode(FORMAT))
                save_info(ID, msg)
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    

def start():
    ID = 1
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
        if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
            thread = threading.Thread(target=handle_client, args=(conn, addr, ID))
            thread.start()
            ID = ID + 1
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


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)

print("[STARTING] Servidor inicializándose...")

#Vaciamos el registro anterior
with open(nom_archivo, 'w') as registro:
    pass

start()
