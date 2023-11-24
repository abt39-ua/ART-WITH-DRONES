import socket
import argparse
import threading
import sys

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 200
ADDR = 0
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ad_weather_ip = 0
ad_weather_port = 0

def obtener_nombre_ciudad(ciudad):
    try:
        # Abrimos el archivo en modo lectura
        with open('ciudades.txt', 'r') as archivo:
            for linea in archivo:
                ciudad_archivo, valor_str = linea.strip().split(':')
                if ciudad.lower() == ciudad_archivo.lower():
                    valor = int(valor_str)
                    return ciudad_archivo, valor
            else:
                return "Ciudad no encontrada", None
    except FileNotFoundError:
        return "Error: El archivo 'ciudades.txt' no se encuentra.", None
    except Exception as e:
        return f"Error: {e}", None

def handle_client(conn, addr):
    try:
        print(f"[NUEVA CONEXIÓN] {addr} se ha conectado.")  # Muestra un mensaje cuando un cliente se conecta

        connected = True
        while connected:
            msg_length = conn.recv(HEADER).decode(FORMAT)  # Recibe la longitud del mensaje
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)  # Recibe el mensaje completo
                if msg == FIN:  # Si el cliente envía "FIN", se desconecta
                    connected = False
                print(f"El cliente [{addr}] solicita información sobre la ciudad: {msg}")

                # Verificamos si la ciudad solicitada es válida
                ciudad, valor = obtener_nombre_ciudad(msg)
                if ciudad != "Ciudad no encontrada" and valor is not None:
                    if valor > 0:
                        conn.send(f"En la ciudad {ciudad} se puede actuar, ya que hacen {valor} grados.".encode(FORMAT)) 
                    else:
                        conn.send(f"CONDICIONES CLIMATICAS ADVERSAS. ESPECTACULO A LA ESPERA O FIINALIZADO. Temperatura: {valor} grados.".encode(FORMAT)) 
                else:
                    respuesta = "Ciudad no válida"

        print(f"ADIOS. Cliente [{addr}] desconectado.") # Mensaje de despedida al cliente
        conn.close()  # Cierra la conexión con el cliente
    except ConnectionResetError:
        print(f"Error de conexión con el cliente [{addr}].")
    except socket.error as e:
        print(f"Error en la comunicación con el cliente [{addr}]: {e}")
    except ValueError:
        print(f"Error: No se pudo convertir un valor a entero.")
    except Exception as e:
        print(f"Error desconocido con el cliente [{addr}]: {e}")
    

# Función principal para iniciar el servidor
def start():
    global server, ad_weather_ip
    try:
        server.listen()  # Empieza a escuchar por conexiones entrantes
        print(f"[ESCUCHANDO] Servidor a la escucha en {ad_weather_ip}")
        
        # Obtiene el número de conexiones activas (hilos de clientes)
        CONEX_ACTIVAS = threading.active_count() - 1
        print(CONEX_ACTIVAS)

        while True:
            conn, addr = server.accept()  # Acepta una nueva conexión entrante
            CONEX_ACTIVAS = threading.active_count()  # Actualiza el número de conexiones activas
            
            if CONEX_ACTIVAS <= MAX_CONEXIONES: 
                # Si no se ha alcanzado el límite de conexiones activas, crea un nuevo hilo para manejar al cliente
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()  # Inicia el hilo para el cliente
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES - CONEX_ACTIVAS)
            else:
                # Si se ha alcanzado el límite, rechaza la conexión y envía un mensaje al cliente
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count() - 1
    # 
    except KeyboardInterrupt:
        print("Servidor detenido por el usuario.")

    # Si los grados asignados a una ciudad no pueden convertirse en entero
    except ValueError:
        print("Error: No se pudo convertir un valor a entero.")

    # Si hay problemas en la creación o enlace del socket
    except socket.error as e:
        print(f"Error en el socket: {e}")
    
    # Cuando se produzca algún error sin especificar anteriormente
    except Exception as e:
        print(f"Error desconocido: {e}")
    finally:
        server.close()

# Crear un socket del servidor y enlazarlo al puerto y dirección.
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Crea un socket de tipo TCP/IP
server.bind(ADDR)  # Enlaza el socket a la dirección y puerto especificados
start()

########################33
import sys
import requests

def main(argv = sys.argv):
    global temp
    ciudad = "London"
    url = "https://api.openweathermap.org/data/2.5/weather?q={}&appid=274d9ed11cbef3a98393a23a34f79bb7&units=metric".format(ciudad)
    res = requests.get(url)
    data = res.json()

    temp = data["main"]["temp"]
    print(f"{temp}")




if __name__ == "__main__":
    main(sys.argv[1:])
    pass