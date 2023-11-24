import socket, ssl

hostname = 'localhost'
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

while True:
    newsocket, fromaddr = bindsocket.accept()
    connstream = context.wrap_socket(newsocket, server_side=True)
    print('Conexion recibida')
    try:
        deal_with_client(connstream)
    finally:
        connstream.shutdown(socket.SHUT_RDWR)
        connstream.close()

