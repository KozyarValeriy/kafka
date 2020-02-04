"""
Producer for sending a messages by kafka.
"""

import numpy as np
from kafka import KafkaProducer

import BuildMessageFabric as Fabric


# Initialization of constants
# Available first and second names
FIRST_NAMES = ['Petya', 'Vanya', 'Grisha', 'Dmitriy', 'Felix', 'Sergey']
SECOND_NAME = ['Petrov', 'Ivanov', 'Sidorov', 'Zelenskiu', 'Zirinovski', 'Medvedev']
# type of messages
TYPE = "JSON"  # "CSV"
# defining builder function
builder = Fabric.Builder(TYPE).get_builder()
# topic name
TOPIC = "kozyar1"

IP = "cdh631.itfbgroup.local"
PORT = "9092"
NUMBER_OF_MESSAGES = 10


def start_chatting():
    producer = KafkaProducer(bootstrap_servers=[f'{IP}:{PORT}'],
                             key_serializer=builder.serializer(), value_serializer=builder.serializer())

    for i in range(NUMBER_OF_MESSAGES):
        try:
            key = 'key' + str(i)
            name = "{0} {1}".format(np.random.choice(FIRST_NAMES), np.random.choice(SECOND_NAME))
            shop = i % 2000 + 1
            traffic = np.random.randint(1000)
            success_sell = abs(traffic - np.random.randint(1000))
            # building message
            values = builder.build_message(name, shop, traffic, success_sell)
            print(values)
            future = producer.send(TOPIC, key=key, value=values)
            record_metadata = future.get(timeout=10)

        except Exception as e:
            print('--> It seems an Error occurred: {}'.format(e))

    producer.flush()


if __name__ == "__main__":
    start_chatting()
