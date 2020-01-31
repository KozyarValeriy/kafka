from kafka import KafkaConsumer
from pyspark.shell import sqlContext

consumer = KafkaConsumer('kozyar1',
                         group_id='my-group',
                         bootstrap_servers=['cdh631.itfbgroup.local:9092'])

for message in consumer:
    print('Key: ' + str(message.key) + ', value: ' + str(message.value) + ', offset: ' + str(message.offset))
    jdbcDF = sqlContext.createDataFrame([(str(message.key), str(bytes.decode(message.value)))], ["key", "value"])
    jdbcDF \
        .write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", 'oracle.jdbc.OracleDriver') \
        .option("url", "jdbc:oracle:thin:@192.168.88.200:1521:orcl") \
        .option("dbtable", "tmp_kafka") \
        .option("user", "kozyar") \
        .option("password", "usertest") \
        .save()
