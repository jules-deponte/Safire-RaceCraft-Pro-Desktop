

from kafka import KafkaProducer
import time
import json
import socket


class RcpKafkaProducer:
    def __init__(self, brokers: list, topic_name: str):
        

        # Listen to the port to which F1 will be sending the data.
        self.UDP_IP = "127.0.0.1"
        self.UDP_PORT = 20777
        
        self.brokers = brokers
        self.topic_name = topic_name



    def json_serializer(self, data):
        return json.dumps(data).encode('utf-8')


    def run_producer(self):
        
        # Connect to the web socket to which this function will be writting.
        self.sock = socket.socket(
            socket.AF_INET, # Internet
            socket.SOCK_DGRAM
        ) # UDP

        self.sock.bind((self.UDP_IP, self.UDP_PORT))
        
        for i in range(3):
            try:
                # Create a Kafka Producer
                self.producer = KafkaProducer( # <==========   Demo
                    bootstrap_servers=self.brokers,
                    value_serializer=self.json_serializer
                    )
                print("Successfully created producer.")
                break
            except:
                print(f"Retrying to create producer, attempt {i+2}")
                time.sleep(5)
        
        
        while True:
            data, addr = self.sock.recvfrom(4096)

            # Change the data from a bytes string into a hex string
            data = data.hex()
            self.producer.send(self.topic_name, data, partition=0)
            # print(data)