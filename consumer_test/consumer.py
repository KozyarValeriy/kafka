"""
Consumer to save data in DB.
"""


from kafka import KafkaConsumer
from pyspark.shell import sqlContext


# Initialization of constants
# topic name
TOPIC = "kozyar1"

IP_DB = "192.168.88.200"
PORT_DB = "1521"

IP_KAFKA = "cdh631.itfbgroup.local"
PORT_KAFKA = "9092"


# Initialization consumer
consumer = KafkaConsumer(TOPIC,
                         group_id='my-group',
                         bootstrap_servers=[f'{IP_KAFKA}:{PORT_KAFKA}'])

for message in consumer:
    # print('Key: ' + str(message.key) + ', value: ' + str(message.value) + ', offset: ' + str(message.offset))
    print("Key: {0}, value: {1}, offset: {2}".format(message.key, message.value, message.offset))
    jdbcDF = sqlContext.createDataFrame([(str(message.key), str(bytes.decode(message.value)))], ["key", "value"])
    jdbcDF \
        .write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", f"jdbc:oracle:thin:@{IP_DB}:{IP_DB}:orcl") \
        .option("dbtable", "tmp_kafka") \
        .option("user", "kozyar") \
        .option("password", "usertest") \
        .save()
