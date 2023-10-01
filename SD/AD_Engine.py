import socket
import sys
import time

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

if  (len(sys.argv) == 5):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    try:
        tiempo_de_consulta = int(sys.argv[4])
    except ValueError:
        print("El tiempo de consulta debe ser un número entero.")
        sys.exit()
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

    msg=sys.argv[3]
    contador = 0
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

# Crear una matriz 2D de 20x20 posiciones para representar el espacio aéreo
espacio_aereo = [[0 for _ in range(20)] for _ in range(20)]

# Imprimir la matriz para visualizar el espacio aéreo
for fila in espacio_aereo:
    print(fila)