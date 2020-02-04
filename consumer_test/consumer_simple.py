"""
Simple consumer for Kafka. Prints messages in console.
"""


from kafka import KafkaConsumer


# Initialization of constants
# topic name
TOPIC = "kozyar"

IP_KAFKA = "cdh631.itfbgroup.local"
PORT_KAFKA = "9092"


# Initialization consumer
consumer = KafkaConsumer(TOPIC,
                         group_id='my-group',
                         bootstrap_servers=[f'{IP_KAFKA}:{PORT_KAFKA}'],
                         key_deserializer=bytes.decode, value_deserializer=bytes.decode)

for message in consumer:
    # print('Key: ' + str(message.key) + ', value: ' + str(message.value) + ', offset: ' + str(message.offset))
    print("Key: {0}, value: {1}, offset: {2}".format(message.key, message.value, message.offset))
