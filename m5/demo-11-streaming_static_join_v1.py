import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window, current_timestamp



if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("Streaming static joins")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    static_schema = StructType([StructField("Country", StringType(), True),\
                                StructField("Year", StringType(), True),\
                                StructField("GDP", DoubleType(), True),\
                                StructField("Population", DoubleType(), True)])

    staticDf = sparkSession.read\
                           .format("csv")\
                           .option("header", "true")\
                           .schema(static_schema)\
                           .load("datasets/lifeExpectancyDataset/static_files/*.csv")


    staticDf.printSchema()

    print("******************************** Static data")
    staticDf.show()


    streaming_schema = StructType([StructField("Country", StringType(), True),\
                                   StructField("Year", StringType(), True),\
                                   StructField("Status", StringType(), True),\
                                   StructField("LifeExpectancy", DoubleType(), True)])


    streamingDf = sparkSession.readStream\
                              .option("header", "true")\
                              .schema(streaming_schema)\
                              .csv("datasets/lifeExpectancyDataset/dropLocation")

    streamingDf.printSchema()

    print("******************************** Streaming data")


    query = streamingDf.writeStream\
                       .outputMode("append")\
                       .format("console")\
                       .start()\
                       .awaitTermination()


















