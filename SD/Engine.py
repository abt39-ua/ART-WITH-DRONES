import sys
from kafka import KafkaProducer
from kafka import KafkaConsumer
import time
import pickle
import threading

class Consumer1(threading.Thread):
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
                #print("Leyendo mensajes de entrypark")

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while True:
            producer1 = KafkaProducer(bootstrap_servers='localhost:9092')
            data1 = pickle.dumps("Coord destino")
            producer1.send('topic_b', data1)

            producer = KafkaProducer(bootstrap_servers='localhost:9092')
            data = pickle.dumps("Mapa")
            producer.send('topic_b', data)

            print("Engine enviando mapa")
            
            producer.close()
            time.sleep(3)

def main(args):
    #producer = get_kafka_producer()

    tasks = [Consumer1(), Producer()]

    for t in tasks:
        t.start()

    while True:

        time.sleep(1)
    

if __name__ == "__main__":
  main(sys.argv[1:])