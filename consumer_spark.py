"""
Consumer for receiving a messages by kafka.
"""

from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import ParserFabric


# Initialization of constants
# type of messages
TYPE = "JSON"  # "CSV"
# defining parser function
parser = ParserFabric.Parser(TYPE).get_parser()

# topic name
TOPIC = "kozyar1"
# window size in seconds
SIZE = 5

NAMES = dict()
IP_DB = "192.168.88.200"
PORT_DB = "1521"

IP_KAFKA = "cdh631.itfbgroup.local"
PORT_KAFKA = "9092"


def last_record(record):
    """ Function to get max record """
    return record[0]


def collect(data):
    """ Function to collect data in RDD """
    global NAMES
    # count, traffic, success_sales
    line = [1, data[2], data[3]]
    print("**********************************************************")
    print(line)
    print(NAMES)
    if data[0] in NAMES:
        old_line = NAMES[data[0]]
        NAMES[data[0]] = [x + y for x, y in zip(line, old_line)]
    else:
        NAMES[data[0]] = line

    print(NAMES)
    print(len(NAMES))
    print("**********************************************************")

    traffic = NAMES[data[0]][1]
    count = NAMES[data[0]][0]
    avg_traffic = traffic / count
    success_sell = NAMES[data[0]][1]
    avg_success_sell = success_sell / count
    return data[0], (count, avg_traffic, avg_success_sell)


def save_data(rdd):
    """ Function for saving data in window """
    global NAMES
    if not rdd.isEmpty():
        # parsing data in RDD
        rdd = rdd \
            .map(lambda x: parser.parse(x[1])) \
            .map(lambda data: collect(data)) \
            .reduceByKey(lambda rec1, rec2: max(rec1, rec2, key=last_record))
        NAMES = dict(rdd.collect())
        print("************************************> NAMES <************************************")
        print(NAMES)
        print("************************************> NAMES <************************************")
        rdd = rdd \
            .map(lambda rec: (rec[0], rec[1][0], rec[1][1], rec[1][2]))
        # create DataFrame and View
        df = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        # query for getting result
        res = spark.sql('select t._1 as NAME, t._2 as COUNT_NAME, t._3 as AVG_TRAFFIC, t._4 as AVG_SUCCESS_SELL from t')
        res.show(40)
        # res.printSchema()
        # res = spark.sql('select count(*) KEY, sum(t._2) VALUE from t')
        res \
            .write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("driver", 'oracle.jdbc.OracleDriver') \
            .option("url", "jdbc:oracle:thin:@{0}:{1}:orcl".format(IP_DB, PORT_DB)) \
            .option("dbtable", "tmp_kafka") \
            .option("user", "kozyar") \
            .option("password", "usertest") \
            .save()
        # spark.catalog.dropTempView("t")
    return rdd


if __name__ == "__main__":
    ssc = StreamingContext(sc, SIZE)

    directKafkaStream = KafkaUtils \
        .createDirectStream(ssc, [TOPIC], {"metadata.broker.list": "{0}:{1}".format(IP_KAFKA, PORT_KAFKA)},
                            keyDecoder=parser.deserializer(), valueDecoder=parser.deserializer())
    directKafkaStream \
        .foreachRDD(lambda x: save_data(x))

    ssc.start()
    ssc.awaitTermination()
