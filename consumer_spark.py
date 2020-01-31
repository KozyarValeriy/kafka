from pyspark.shell import spark, sc, sqlContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import ParserFabric

TYPE = "CSV"
NAMES = dict()


def collect(data):
    global NAMES
    # count, traffic, success_sales
    line = [1, data[2], data[3]]
    print("----------------------------------------------------------")
    print(line)
    if data[0] in NAMES:
        old_line = NAMES[data[0]]
        NAMES[data[0]] = [x + y for x, y in zip(line, old_line)]
    else:
        NAMES[data[0]] = line

    traffic = NAMES[data[0]][1]
    count = NAMES[data[0]][0]
    avg_traffic = traffic / count
    success_sales = NAMES[data[0]][1]
    avg_success_sales = success_sales / count
    return data[0], count, avg_traffic, avg_success_sales


def save_data(rdd):
    if not rdd.isEmpty():
        # parsing data
        rdd = rdd \
            .map(lambda x: parse(x[1])) \
            .map(lambda data: collect(data))
        df = sqlContext.createDataFrame(rdd)
        df.createOrReplaceTempView("t")
        res = spark.sql('select * from t')
        res.show()
        res.printSchema()
        # res = spark.sql('select count(*) KEY, sum(t._2) VALUE from t')
        res \
            .write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("driver", 'oracle.jdbc.OracleDriver') \
            .option("url", "jdbc:oracle:thin:@192.168.88.200:1521:orcl") \
            .option("dbtable", "tmp_kafka") \
            .option("user", "kozyar") \
            .option("password", "usertest") \
            .save()

    print("------------------------------------------------------")
    print(NAMES)
    print("------------------------------------------------------")
    return rdd


if __name__ == "__main__":
    parser = ParserFabric.Parser(TYPE)
    parse = parser.get_parser().parse

    ssc = StreamingContext(sc, 5)

    broker_list = 'cdh631.itfbgroup.local:9092'
    topic = "kozyar1"

    directKafkaStream = KafkaUtils.createDirectStream(ssc,
                                                      [topic],
                                                      {"metadata.broker.list": broker_list})
    directKafkaStream.foreachRDD(lambda x: save_data(x))

    ssc.start()
    ssc.awaitTermination()
