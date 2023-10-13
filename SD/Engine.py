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

        consumer2.subscribe(['pos'])

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
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        data = pickle.dumps("Engine enviando")

        print("lola")
        
        producer.send('topic_a', data)

def main(args):
    try:
        topic = args[0]
        key = args[1]
        message = args[2]
    except Exception as ex:
        print("Failed to set topic, key, or message")

    #producer = get_kafka_producer()
    #publish(producer, topic, key, message)

    tasks = [Consumer1(), Producer()]

    for t in tasks:
        t.start()

    while True:

        time.sleep(1)


def publish(producer_instance, topic_name, key, value):
    try:
        while True:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
            producer_instance.flush()
            print(f"Publish Succesful ({key}, {value}) -> {topic_name}")

            time.sleep(2)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def get_kafka_producer(servers=['localhost:9092']):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=servers, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    

if __name__ == "__main__":
  main(sys.argv[1:])