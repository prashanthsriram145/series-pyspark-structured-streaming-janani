from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp



if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("Triggers")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("car", StringType(), True),\
                         StructField("price", DoubleType(), True),\
                         StructField("body", StringType(), True),\
                         StructField("mileage", DoubleType(), True),\
                         StructField("engV", StringType(), True),\
                         StructField("engType", StringType(), True),\
                         StructField("registration", StringType(), True),\
                         StructField("year", IntegerType(), True),\
                         StructField("model", StringType(), True),\
                         StructField("drive", StringType(), True)])


    fileStreamDf = sparkSession.readStream\
                     .option("header", "true")\
                     .option("maxFilesPerTrigger", 1)\
                     .schema(schema)\
                     .csv("datasets/carAdsDataset/droplocation")


    fileStreamDf.printSchema()

    fileStreamDf = fileStreamDf.withColumn("timestamp", current_timestamp())

    aggregationDf = fileStreamDf.groupBy("body",)\
                                .agg({'price': 'avg', 'timestamp': 'max'})\
                                .withColumnRenamed("avg(price)", "avg_price")\
                                .withColumnRenamed("max(timestamp)", "max_timestamp")\
                                .orderBy("avg_price")


    query = aggregationDf.writeStream\
                     .outputMode("complete")\
                     .trigger(processingTime='60 seconds') \
                     .format("console")\
                     .option("numRows", 15)\
                     .option("truncate", "false")\
                     .start()\
                     .awaitTermination()

