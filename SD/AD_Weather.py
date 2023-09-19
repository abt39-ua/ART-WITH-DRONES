import socket
import argparse

# Datos de las ciudades (puedes personalizar esta lista)
ciudades = {
    "madrid": "Madrid, España",
    "paris": "París, Francia",
    "berlin": "Berlín, Alemania",
    "roma": "Roma, Italia",
    "londres": "Londres, Reino Unido",
}

def obtener_nombre_ciudad(ciudad):
    return ciudades.get(ciudad.lower(), "Ciudad no encontrada")

def main():
    parser = argparse.ArgumentParser(description="Servidor de Ciudad")
    parser.add_argument("puerto", type=int, help="Puerto de escucha")
    args = parser.parse_args()

    host = "127.0.0.1"  # Dirección IP del servidor

    # Crear un socket TCP/IP
    servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Enlazar el socket al host y puerto especificados
    servidor_socket.bind((host, args.puerto))

    # Escuchar conexiones entrantes (máximo 5 conexiones en espera)
    servidor_socket.listen(5)

    print(f"Servidor escuchando en {host}:{args.puerto}")

    while True:
        # Aceptar una conexión entrante
        cliente_socket, cliente_direccion = servidor_socket.accept()
        print(f"Conexión entrante de {cliente_direccion}")

        # Recibir datos del cliente
        datos = cliente_socket.recv(1024).decode("utf-8").strip()

        if datos:
            print(f"Peticion recibida: {datos}")
            ciudad = obtener_nombre_ciudad(datos)
            respuesta = f"Nombre de la ciudad: {ciudad}"
            cliente_socket.send(respuesta.encode("utf-8"))
        else:
            respuesta = "Peticion vacia"

        # Cerrar la conexión con el cliente
        cliente_socket.close()

if __name__ == "__main__":
    main()
