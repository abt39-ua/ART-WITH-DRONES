import socket
import sys
import signal
import os
import sys

ID=0
alias = ""

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando conexión...")
    client.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)

def send(msg,client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
########## MAIN ##########
def start():
    if  (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        ADDR = (SERVER, PORT)
        
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")

        msg=sys.argv[3]
        while msg != FIN :
            print("Realizando solicitud al servidor")
            send(msg, client)
            print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
            recibido = client.recv(2048).decode(FORMAT)
            print(f"{recibido}")
            ID = recibido[]
            alias = msg
            print(f"{ID}, {alias}")
            msg=input()

        print ("SE ACABO LO QUE SE DABA")
        print("Envio al servidor: ", FIN)
        send(FIN)
        client.close()
    else:
        print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Alias deseado>")

start()
