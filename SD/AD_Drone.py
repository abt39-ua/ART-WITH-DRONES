import json
import socket
import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pickle
import time
import threading
import signal

ID=0
alias = ""

HEADER = 64

FORMAT = 'utf-8'
FIN = "FIN"
ca_x = 0
ca_y = 0
cd_x = -1
cd_y = -1
ad_engine_ip = ""
ad_engine_port = 0
broker_ip = ""
broker_port = 0
ad_registry_ip = ""
ad_registry_port = 0
destino_alcanzado = False

semaforo = threading.Semaphore(value=1)

def handle_interrupt(signum, frame):
    global ID
    print(f"Cerrando conexión...")
    sys.exit(0)

# Manejar la señal SIGINT (CTRL+C)
signal.signal(signal.SIGINT, handle_interrupt)

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry():
    global ID
    alias=input("Introduce tu alias: ")
    ADDR = (ad_registry_ip, ad_registry_port)  
        
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(ADDR)
        print (f"Establecida conexión en [{ADDR}]")

        print("Realizando solicitud al servidor")
        send(alias, client)
        mensaje = client.recv(2048).decode(FORMAT)
        partes_mensaje = mensaje.split(": ")
        if(len(partes_mensaje) == 2):
            ID = int(partes_mensaje[1])
        else:
            ID = int(partes_mensaje[0])
        print(f"Recibo del Servidor: {ID}")
        print()
    except (ConnectionRefusedError, TimeoutError):
        print("No ha sido posible establecer conexión con el servidor de registro, inténtelo de nuevo.")
    
    except Exception as e:
        print(f"Error: {e}")

<<<<<<< HEAD
    finally:
        if 'client' in locals():
            client.close()
    
# Códigos de escape ANSI para colores de fondo
FONDO_ROJO = "\033[41m"
FONDO_VERDE = "\033[102m"
FONDO_CREMA = "\033[48;5;224m"
RESET = "\033[0m"
TEXTO_NEGRO = "\033[30m"
LETRA_GROSOR_NEGRITA = "\033[1m"
=======
        #msg=sys.argv[3]
    print("Realizando solicitud al servidor")
    send(alias, client)
    ID = client.recv(2048).decode(FORMAT)
    #print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
    print(f"Recibo del Servidor: {ID}")
    #ID = client.recv(2048).decode(FORMAT)
>>>>>>> 0b1fc5c80fef3de0e5f65a416f7af50fa9e816d7



cuadrado = "□"


def imprimir_mapa_actualizado(mapa, figura):
    global completada
    n = 0
    for y in range(20):
        for x in range(20):
            drones_en_casilla = []
            for id, posicion in mapa.items():
                if posicion == (x, y):
                    drones_en_casilla.append(id)
            cantidad_drones = len(drones_en_casilla)
            n = max(n, cantidad_drones)  # Actualiza n con el máximo número de drones encontrados en una casilla
    
    longitud_maxima = max(len(cuadrado),(n))        
    figura_ajustada = {
        int(i): (x - 1, y - 1) for i, (x, y) in figura.items()
    }

    volver_base = {id: (1, 1) for id in figura.keys()}
    # Imprimir línea de números del 1 al 20
    if mapa == figura_ajustada and figura_ajustada != volver_base:
        completada = True
        encabezado = "*********** ART WITH DRONES **********"
        tablero_width = 20 * (longitud_maxima + 3)  # Tamaño total del tablero (20 filas, cada una con longitud_maxima y un espacio)
        encabezado_centralizado = encabezado.center(tablero_width)
        print(encabezado_centralizado)
        mensaje = "FIGURA COMPLETADA"
        mensaje_centralizado = mensaje.center(tablero_width)
        print(FONDO_CREMA + LETRA_GROSOR_NEGRITA + TEXTO_NEGRO + mensaje_centralizado + RESET)

    if(longitud_maxima > 2):
        print("   " + "  ".join(str(i).rjust(longitud_maxima*2-2) for i in range(1, 21)))
    else:
        print("   " + " ".join(str(i).rjust(3) for i in range(1, 21)))
    
    for x in range(20):
        # Imprimir número de la fila
        print(str(x+1).rjust(2), end=" ")
        for y in range(20):
            drones_en_casilla = []
            id_dron = None
            for id, posicion in mapa.items():
                if posicion == (x, y):
                    drones_en_casilla.append(id)
                
            if(mapa == figura_ajustada):
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima != 1): 
                        if(longitud_maxima-len(drones_en_casilla)) != 0:
                            print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                        else:
                            print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                    else:
                        if(longitud_maxima-len(drones_en_casilla)) != 0:
                            print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                        else:
                            print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                else:
                    if(longitud_maxima != 1):
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                        print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
                    else:
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima*2)
                        print(cuadrado_formateado, end=" "*longitud_maxima*2)

            else:
                if drones_en_casilla:
                    numeros_drones = ' '.join(str(id_dron) for id_dron in drones_en_casilla)
                    numero_formateado = numeros_drones.rjust(longitud_maxima)
                    if(longitud_maxima != 1): 
                        if len(drones_en_casilla) == 1 and figura_ajustada.get(int(drones_en_casilla[0])) == (x, y):
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                        else:
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" ")
                    else:
                        if len(drones_en_casilla) == 1 and figura_ajustada.get(int(drones_en_casilla[0])) == (x, y):
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(' ' + FONDO_VERDE + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                        else:
                            if(longitud_maxima-len(drones_en_casilla)) != 0:
                                print(FONDO_ROJO + numero_formateado + RESET, end=" "*(longitud_maxima))
                            else:
                                print(' ' + FONDO_ROJO + TEXTO_NEGRO + LETRA_GROSOR_NEGRITA + numero_formateado + RESET, end=" "*2)
                else:
                    if(longitud_maxima != 1): 
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima)
                        print(cuadrado_formateado, end=" "*longitud_maxima)  # Imprimir espacio en blanco si no hay dron en esa posición
                    else:
                        cuadrado_formateado = cuadrado.rjust(longitud_maxima*2)
                        print(cuadrado_formateado, end=" "*longitud_maxima*2) 
        print()  # Nueva línea para la siguiente fila
    print() 

        
#######   ENGINE   #######

class Producer(threading.Thread):
    global ID, ca_x, ca_y
    ca_x = 0
    ca_y = 0
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()
        self.last_sent_message = None  # Almacena el último mensaje enviado a Kafka

    def stop(self):
        self.stop_event.set()

    def run(self):
        global semaforo, destino_alcanzado
        while not self.stop_event.is_set():
            producer = KafkaProducer(bootstrap_servers=self.broker_address)

            position_data = (ID, (ca_x, ca_y))
            if ca_x == cd_x and ca_y == cd_y:
                if destino_alcanzado == False:
                    print("He llegado a mi destino")
                    destino_alcanzado = True
            else:
                destino_alcanzado = False

            if position_data != self.last_sent_message:
                print(f"Me he movido a ({ca_x+1}, {ca_y+1})")
                semaforo.acquire()
                serialized_position = pickle.dumps(position_data)
                producer.send('topic_a', serialized_position)

                # Almacenar el nuevo mensaje como el último mensaje enviado
                self.last_sent_message = position_data

                # Liberar el permiso del semáforo
                semaforo.release()
            time.sleep(1)
            producer.close()

def setCoords(x, y):
    global cd_x, cd_y
    cd_x = x
    cd_y = y
    mov(cd_x, cd_y, ca_x, ca_y)

def mov(cd_x, cd_y, x, y):
    global ca_x, ca_y 
    ca_x = x
    ca_y = y

    width = 20
    height = 20

    # Calcula las distancias en la dirección x y ajusta para la geometría esférica
    distancia_x = cd_x - ca_x
    if distancia_x > width / 2:
        distancia_x -= width
    elif distancia_x < -width / 2:
        distancia_x += width

    # Calcula las distancias en la dirección y y ajusta para la geometría esférica
    distancia_y = cd_y - ca_y
    if distancia_y > height / 2:
        distancia_y -= height
    elif distancia_y < -height / 2:
        distancia_y += height

    if ca_x == cd_x and ca_y == cd_y:
        return 1
    else:
            # Mueve las posiciones de 1 en 1 y ajusta para la geometría esférica
        if distancia_x > 0:
            ca_x += 1
        elif distancia_x < 0:
            ca_x -= 1

        if distancia_y > 0:
            ca_y += 1
        elif distancia_y < 0:
            ca_y -= 1
        # Asegura que las posiciones estén dentro de los límites del mapa
        ca_x %= width
        ca_y %= height

class Consumer(threading.Thread):
    global ID
    def __init__(self):
        threading.Thread.__init__(self)
        self.broker_address = f"{broker_ip}:{broker_port}"
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        try:
            mapa_anterior = None
            figura_anterior = None
            consumer_coord = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            consumer_mapa = KafkaConsumer(bootstrap_servers=self.broker_address ,
                                                auto_offset_reset='latest',
                                                consumer_timeout_ms=1000, value_deserializer=lambda x: pickle.loads(x))
            
            consumer_coord.subscribe(['topic_coord'])
            consumer_mapa.subscribe(['topic_mapa'])

            while not self.stop_event.is_set():
                for coor_message in consumer_coord:

                    datos_coor = coor_message.value
                    for id, (x, y) in datos_coor.items():
                        if ID == int(id):
                            print(f'ID: {id}, Coordenadas:({x}, {y})')
                            print()
                            time.sleep(2)
                        
                            semaforo.acquire()
                            setCoords(x-1,y-1)
                            
                            for mapa_message in consumer_mapa:
                                datos_mapa = mapa_message.value
                                mapa_recibido = datos_mapa.get("mapa")
                                figura_recibida = datos_mapa.get("figura")
                                if(figura_anterior == None):
                                    if(mapa_recibido != mapa_anterior):
                                        imprimir_mapa_actualizado(mapa_recibido, figura_recibida)
                                else:
                                    if(mapa_recibido != mapa_anterior):
                                        imprimir_mapa_actualizado(mapa_recibido, figura_anterior)
                                mapa_anterior = mapa_recibido
                                figura_anterior = figura_recibida
                                
                            # Liberar el semáforo después de la actualización del mapa
                            semaforo.release()
                if self.stop_event.is_set():
                    break

        except Exception as e:
            print(f"Error en el consumidor: {e}")

########## MAIN ##########

def main(argv = sys.argv):
    if len(sys.argv) != 7:
        print("Error: El formato debe ser el siguiente: [IP_Engine] [Puerto_Engine] [IP_Broker] [Puerto_Broker] [IP_Registry] [Puerto_Registry]")
        sys.exit(1)
    else:  
        global ad_engine_ip, ad_engine_port, broker_ip, broker_port, ad_registry_ip, ad_registry_port, ID
        
        ad_engine_ip = sys.argv[1]
        ad_engine_port = int(sys.argv[2])
        broker_ip = sys.argv[3]
        broker_port = int(sys.argv[4])
        ad_registry_ip = sys.argv[5]
        ad_registry_port = int(sys.argv[6])
        orden = ""
        while(orden != "3"):
            
            print("¿Qué deseas hacer?")
            print("1. Registrarse")
            print("2. Empezar representación")
            print("3. Apagarse")
            print("Opción:", end=" ")
            orden = input()
            print()
            
            while orden != "1" and orden != "2" and orden != "3":
                print("Error, indica una de las 3 posibilidades por favor(1, 2 o 3).")
                print("Opción:", end=" ")
                orden = input()
                print()

            if orden == "1":
                if(ID != 0):
                    print("Ya estás registrado!")
                    print()
                else:
                    registry()

            if orden == "2":
                if(ID == 0):
                    print("No estás registrado!")
                    print()
                else:
                    tasks = [Consumer(), Producer()]

                    for t in tasks:
                        t.start()

                    while True:
                        time.sleep(1)

            if orden == "3":
                sys.exit()

if __name__ == "__main__":
  main(sys.argv[1:])