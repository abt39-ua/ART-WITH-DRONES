#-Conexión con weather
#-conexión con el fichero de dibujo y obtención num de drones
#-comprobar num drones en registro.txt
#-enviar cada pos requerida a cada dron
#-expresion del mapa a cada mov de dron


import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
########## MAIN ##########8

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
        send(msg)
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

