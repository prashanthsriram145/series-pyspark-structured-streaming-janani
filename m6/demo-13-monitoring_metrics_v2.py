import pprint

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window



if __name__ == "__main__":
    sparkSession = SparkSession \
        .builder \
        .appName("Windowing operations")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("Date", TimestampType(), True),
                         StructField("Open", DoubleType(), True),
                         StructField("High", DoubleType(), True),
                         StructField("Low", DoubleType(), True),
                         StructField("Close", DoubleType(), True),
                         StructField("Adjusted Close", DoubleType(), True),
                         StructField("Volume", DoubleType(), True),
                         StructField("Name", StringType(), True)
                         ])

    stockPricesDf = sparkSession \
            .readStream \
            .option("header", "true") \
            .schema(schema) \
            .csv("datasets/stock_data")

    print(" ")
    print(stockPricesDf.printSchema())


    averageCloseDf = stockPricesDf \
            .groupBy("Name") \
            .agg({"Close": "avg"}) \
            .withColumnRenamed("avg(Close)", "Average Close")


    query = averageCloseDf \
            .writeStream.outputMode("complete") \
            .queryName("aggregations") \
            .option("checkpointLocation", "checkpointDir") \
            .format("console") \
            .trigger(once=True)\
            .option("truncate", "false") \
            .start()

    query.awaitTermination()

    pprint.pprint(query.status)
    pprint.pprint(query.recentProgress)
    pprint.pprint(query.lastProgress)

    query.stop()
