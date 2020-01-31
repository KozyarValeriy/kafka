import numpy as np
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                         key_serializer=str.encode, value_serializer=str.encode)

FIRST_NAMES = ['Petya', 'Vanya', 'Grisha', 'Dmitriy', 'Felix', 'Sergey']
SECOND_NAME = ['Petrov', 'Ivanov', 'Sidorov', 'Zelenskiu', 'Zirinovski', 'Medvedev']

for i in range(1000):
    try:

        key = 'key' + str(i)
        name = "{0} {1}".format(np.random.choice(FIRST_NAMES), np.random.choice(SECOND_NAME))
        shop = i % 2000 + 1
        traffic = np.random.randint(1000)
        success_sell = abs(traffic - np.random.randint(1000))
        values = ",".join([name, str(shop), str(traffic), str(success_sell)])

        future = producer.send("kozyar1", key=key, value=values)
        record_metadata = future.get(timeout=10)

    except Exception as e:
        print('--> It seems an Error occurred: {}'.format(e))

producer.flush()
