#-Conexión con weather
#-conexión con el fichero de dibujo y obtención num de drones
#-comprobar num drones en registro.txt
#-enviar cada pos requerida a cada dron
#-expresion del mapa a cada mov de dron


import socket
import sys
import threading
import os

HEADER = 64
PORT = 5051
FORMAT = 'utf-8'
FIN = "FIN"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
MAX_CONEXIONES = 10

temp =0

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def getWeather():
    if  (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (SERVER, PORT)
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")

        msg=sys.argv[3]
        while msg != FIN :
            print("Envio al servidor: ", msg)
            send(msg, client)
            print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
            msg=input()
        print("Envio al servidor: ", FIN)
        send(FIN)
        client.close()
    else:
        print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Ciudad>")

    # Crear una matriz 2D de 20x20 posiciones para representar el espacio aéreo
    espacio_aereo = [[0 for _ in range(20)] for _ in range(20)]

    # Imprimir la matriz para visualizar el espacio aéreo
    for fila in espacio_aereo:
        print(fila)

def getFigura(figura):
    with open(figura, 'r') as file:
        for linea in file:
            partes = linea.strip().split()
            if len(partes) == 3:
                id = partes[0]
                x = float(partes[1])
                y = float(partes[2])
                datos[id] = (x, y)
    return datos

def getCoord(id):
    if id in datos:
        coord = datos[id]
        print(f"X: {coord[0]}, Y: {coord[1]}")
    else:
        print("No se han encontrado el ID en la figura.")

def connectDron(nDrones): 
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

    #Buscar cood del id


def handle_client(conn, addr, ID):
    print(f"[NUEVA CONEXION] {addr} connected.")
    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = int(conn.recv(msg_length).decode(FORMAT))
            if msg < 0:
                connected = False
            else:
                print(f"He recibido del cliente [{addr}] el id: {msg}")
                coord = getCoord(msg)
                x = coord[0]
                y = coord[1]
                conn.send(f"X: {x} Y: {y}".encode(FORMAT))

    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()


########## MAIN ##########

print("Consiguiendo información del tiempo...")
getWeather()
print("Consiguiendo figura...")
datos = {}
getFigura("Figura.txt")
print("Conectando con drones...")
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)
print("[STARTING] Servidor inicializándose...")
connectDron()
