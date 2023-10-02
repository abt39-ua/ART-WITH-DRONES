#-Conexión con weather
#-conexión con el fichero de dibujo y obtención num de drones
#-comprobar num drones en registro.txt
#-enviar cada pos requerida a cada dron
#-expresion del mapa a cada mov de dron


import socket
import sys
import threading
import time
import os

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
MAX_CONEXIONES = 10

temp =0
output_lock = threading.Lock()

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def getWeather(msg):
    while True:
        msg = ciudad
        with output_lock:
            if  (len(sys.argv) == 3):
                SERVER = sys.argv[1]
                PORT = int(sys.argv[2])
                ADDR = (SERVER, PORT)
                
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(ADDR)
                print (f"Establecida conexión en [{ADDR}]")

                #msg=input("¿En que ciudad se va a actuar?")
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

        time.sleep(10)


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


########## MAIN ##########

msg=input("¿En que ciudad se va a actuar?")
ciudad = msg
print("Consiguiendo información del tiempo...")
#getWeather()
thread =threading.Thread(target = getWeather, args = (msg,))
thread.daemon = True
thread.start()
while True:
    awa = input("bu")
    print("Consiguiendo figura...")
    datos = {}
    getFigura("Figura.txt")
    time.sleep(60)
    #connectDron()
