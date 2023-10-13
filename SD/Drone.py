import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
import pickle
import threading

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        while True:
            producer = KafkaProducer(bootstrap_servers="localhost:9092")
            data = pickle.dumps("Posición a la que me muevo")
            producer.send('topic_a', data)

            print("Me he movido")

            producer.close()
            time.sleep(4)

class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        grupo = '1'
        consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092',
                                            auto_offset_reset='latest',
                                            consumer_timeout_ms=1000, group_id = grupo)

        consumer2.subscribe(['topic_b'])

        while not self.stop_event.is_set():
            for message in consumer2:
                print(pickle.loads(message.value))

                if self.stop_event.is_set():
                    break
                #print("Leyendo mensajes de entrypark")


def main(args):

    tasks = [Consumer(), Producer()]

    for t in tasks:
        t.start()

    while True:

        time.sleep(1)


if __name__ == "__main__":
  main(sys.argv[1:])