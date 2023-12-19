"""
import socket
import ssl

hostname = '127.0.0.1'
port = 8443

# Se crea el contexto para el cliente indicándole que confie en certificados autofirmados
#context = ssl.create_default_context()
context = ssl._create_unverified_context()

with socket.create_connection((hostname, port)) as sock:
    with context.wrap_socket(sock, server_hostname=hostname) as ssock:
        print(ssock.version()) #TLSv1.3
        print(ssock.getpeername()) #('127.0.0.1', 8443) Server
        print(ssock.getsockname()) #('127.0.0.1', 60605) Client     
        print('Enviando HOLA MUNDO')
        ssock.send(b'HOLA MUNDO');
        data = ssock.recv(1024)
        print('Recibido', repr(data))
"""

import ssl
import socket

def client():
    client_cert = 'cert.pem'

    # Crear un socket TCP/IP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Conectar el socket al servidor
    server_address = ('127.0.0.1', 8443)
    client_socket.connect(server_address)

    # Configurar SSL
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations(cafile=client_cert)

    # Configurar el socket para realizar la conexión SSL
    client_socket = context.wrap_socket(client_socket, server_hostname='127.0.0.1')

    try:
        # Enviar datos
        message = "Hola, servidor!"
        client_socket.sendall(message.encode('utf-8'))

    finally:
        # Cerrar la conexión
        client_socket.close()

if __name__ == "__main__":
    client()
