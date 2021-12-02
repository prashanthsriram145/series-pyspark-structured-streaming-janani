from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, window


def main():

    sparkSession = SparkSession \
        .builder \
        .appName("Monitoring operations")\
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
            .option("truncate", "false") \
            .start()

    print("******Query id (remains same across runs, stored in the checkpoint): ", query.id)
    print("******Query run id (new one generated each run): ", query.runId)        
    print("******Query name (assigned by user): ", query.name)        

    query.awaitTermination()


if __name__ == '__main__':
    main()

