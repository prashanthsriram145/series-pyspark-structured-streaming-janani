import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, window



if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("UserDefinedFunctions")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("Invoice ID", StringType(), True),\
    					 StructField("Branch", StringType(), True),\
    					 StructField("City", StringType(), True),\
    					 StructField("Customer type", StringType(), True),\
    					 StructField("Gender", StringType(), True),\
    					 StructField("Product line", StringType(), True),\
    					 StructField("Unit price", DoubleType(), True),\
    					 StructField("Quantity", IntegerType(), True),\
    					 StructField("Tax 5%", DoubleType(), True),\
               StructField("Total", DoubleType(), True),\
               StructField("Date", StringType(), True),\
               StructField("Time", StringType(), True),\
               StructField("Payment", StringType(), True),\
               StructField("cogs", DoubleType(), True),\
               StructField("gross margin percentage", DoubleType(), True),\
               StructField("gross income", DoubleType(), True),\
    					 StructField("Rating", DoubleType(), True)])


    fileStreamDf = sparkSession.readStream\
                               .option("header", "true")\
                               .schema(schema)\
                               .csv("datasets/supermarketSalesDataset/dropLocation")

    fileStreamDf.printSchema()


    def get_timestamp(date_string, time_string):

        datetime_string = date_string + " " + time_string

        date_time = datetime.datetime.strptime(datetime_string, "%m/%d/%Y %H:%M")

        return date_time


    add_datetime_udf = udf(get_timestamp, TimestampType())

    fileStreamDf = fileStreamDf.withColumn("Datetime", add_datetime_udf("Date", "Time"))


    averagePriceWindowDf = fileStreamDf.groupBy(window(fileStreamDf.Datetime, "1 day"), "Product line")\
                                       .agg({"Unit price": "avg"})

    averagePriceDf = averagePriceWindowDf \
            .orderBy(averagePriceWindowDf.window.start, "Product line")

    query = averagePriceDf.writeStream\
                          .outputMode("complete")\
                          .format("console")\
                          .option("truncate", "false")\
                          .start()\
                          .awaitTermination()




















