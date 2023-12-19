"""
import socket, ssl

hostname = "127.0.0.1"
port = 8443
cert = 'certServ.pem'

# Comando de generación de certificado autofirmado mediante openssl
# openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout certServ.pem -out certServ.pem

# Se crea el contexto
#context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

# Se carga el certificado con clave pública y privada
#context.load_cert_chain(certfile="mycertfile", keyfile="mykeyfile")
context.load_cert_chain(cert, cert)

bindsocket = socket.socket()
bindsocket.bind((hostname, port))
bindsocket.listen(5)

# Funcion que maneja cada conexion
def deal_with_client(connstream):
    data = connstream.recv(1024)
    # empty data means the client is finished with us    
    print('Recibido ', repr(data))
    #data = connstream.recv(1024)
    print("Enviando ADIOS")      
    connstream.send(b'ADIOS')
    
print('Escuchando en',hostname, port)

conected = True
while conected:
    newsocket, fromaddr = bindsocket.accept()
    connstream = context.wrap_socket(newsocket, server_side=True)
    print('Conexion recibida')
    #try:
    deal_with_client(connstream)
    #finally:
        #connstream.shutdown(socket.SHUT_RDWR)
        #connstream.close()
        #conected = False

"""

import ssl
import socket

def server():
    server_cert = 'cert.pem'
    server_key = 'key.pem'

    # Crear un socket TCP/IP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Enlazar el socket al puerto
    server_address = ('127.0.0.1', 8443)
    server_socket.bind(server_address)

    # Configurar SSL
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=server_cert, keyfile=server_key)

    # Configurar el socket para aceptar conexiones SSL
    server_socket = context.wrap_socket(server_socket, server_side=True)

    # Escuchar conexiones entrantes
    server_socket.listen(1)

    print(f"Servidor escuchando en {server_address}")

    while True:
        # Esperar a una conexión
        connection, client_address = server_socket.accept()
        try:
            print(f"Conexión desde {client_address}")

            # Recibir datos
            data = connection.recv(1024)
            print(f"Datos recibidos: {data.decode('utf-8')}")

        finally:
            # Cerrar la conexión
            connection.close()

if __name__ == "__main__":
    server()
