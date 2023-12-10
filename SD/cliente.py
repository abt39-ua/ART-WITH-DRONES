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