from kafka import KafkaConsumer

consumer = KafkaConsumer('kozyar',
                         group_id='my-group',
                         bootstrap_servers=['cdh631.itfbgroup.local:9092'],
                         key_deserializer=bytes.decode, value_deserializer=bytes.decode)

for message in consumer:
    print('Key: ' + str(message.key) + ', value: ' + str(message.value) + ', offset: ' + str(message.offset))
