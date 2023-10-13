import socket
import sys
from kafka import KafkaConsumer

ID=0
alias = ""

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"


#######  REGISTRY   ######

def send(msg, client):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
def registry():
    msg =input("Introduce tu alias")
    #if  (len(sys.argv) == 4):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión en [{ADDR}]")

        #msg=sys.argv[3]
    while msg != FIN :
        print("Realizando solicitud al servidor")
        send(msg, client)
        ID = client.recv(2048).decode(FORMAT)
        #print("Recibo del Servidor: ", client.recv(2048).decode(FORMAT))
        print(f"Recibo del Servidor: {ID}")
        #ID = client.recv(2048).decode(FORMAT)
        msg=input()

    print(f"{ID}")
    print ("SE ACABO LO QUE SE DABA")
    print("Envio al servidor: ", FIN)
    send(FIN)
    client.close()
    #else:
    #    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Alias deseado>")


#######   ENGINE   #######

def subscribe(consumer_instance):
    try:
        for event in consumer_instance:
            key = event.key.decode("utf-8")
            value = event.value.decode("utf-8")
            print(f"Message Received: ({key}, {value})")
        consumer_instance.close()
    except Exception as ex:
        print('Exception in subscribing')
        print(str(ex))

def get_kafka_consumer(topic_name, servers=['localhost:9092']):
    _consumer = None
    try:
        _consumer = KafkaConsumer(topic_name, auto_offset_reset='latest', bootstrap_servers=servers, api_version=(0, 10), consumer_timeout_ms=10000)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


########## MAIN ##########

def main(args):
    print("¿Qué deseas hacer?")
    print("1. Registrarse")
    print("2. Empezar representación")
    print("3. Apagarse")
    orden = input()

    while orden != "1" and orden != "2" and orden != "3":
        print("Error, indica una de las 3 posibilidades por favor(1, 2 o 3).")
        orden = input()

    if orden == "1":
        registry()
    if orden == "2":
        try:
            topic = args[0]
        except Exception as ex:
            print("Failed to set topic")

        consumer = get_kafka_consumer(topic)
        subscribe(consumer)

    if orden == "3":
        sys.exit()

if __name__ == "__main__":
  main(sys.argv[1:])