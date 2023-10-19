import sys
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import pickle
import threading
import signal

def signal_handler(sig, frame):
    # Tareas de limpieza aquí, si es necesario
    print("Cerrando conexión...")
    client.close()  # Cierra el socket del servidor
    sys.exit(0)  # Sale del programa

# Asigna el manejador de señales
signal.signal(signal.SIGINT, signal_handler)

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000)

        consumer2.subscribe(['topic_a'])

        while not self.stop_event.is_set():
            for message in consumer2:
                print(pickle.loads(message.value))

                if self.stop_event.is_set():
                    break

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        id_counter = 1
        while not self.stop_event.is_set():
            try:
                # Crear un objeto KafkaProducer
                producer = KafkaProducer(bootstrap_servers='localhost:9092')

                # Coordenadas (x, y) con un ID
                x = 10  # Ejemplo: coordenada x
                y = 20  # Ejemplo: coordenada y
                position_id = id_counter  # ID único para la posición
                id_counter += 1  # Incrementar el contador de ID

                # Estructura de datos con ID y coordenadas
                position_data = {
                    'id': position_id,
                    'x': x,
                    'y': y
                }

                # Serializar la estructura de datos y enviar al topic 'topic_b'
                serialized_position = pickle.dumps(position_data)
                producer.send('topic_b', serialized_position)

                print(f"Posición enviada: {position_data}")

            except Exception as e:
                print(f"Error: {e}")

            finally:
                # Cerrar el productor
                producer.close()

            # Esperar 3 segundos antes de enviar la siguiente posición
            time.sleep(3)



#producer = get_kafka_producer()

tasks = [Consumer(), Producer()]

for t in tasks:
    t.start()

while True:

    time.sleep(1)

    
