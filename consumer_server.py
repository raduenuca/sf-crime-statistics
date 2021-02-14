from argparse import ArgumentParser
from kafka import KafkaConsumer
import json

def setup_kafka_consumer(server="localhost:9092"):
    consumer = KafkaConsumer(bootstrap_servers=[server],
                             auto_offset_reset='earliest',
                             group_id="consumer_server",
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    return consumer


def consume(topic, server):
    consumer = setup_kafka_consumer(server)
    consumer.subscribe([topic])
    
    for message in consumer:
        print(message.value)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("topic", help="The topic id to consume on.", metavar="<String: topic>")
    parser.add_argument("--bootstrap-server", dest="bootstrapServer", help="The server(s) to connect to.", metavar="<String: server to connect to>", default="localhost:9092")

    args = parser.parse_args()
    consume(topic=args.topic, server=args.bootstrapServer)
