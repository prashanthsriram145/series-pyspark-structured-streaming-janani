from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("trigger_aggregations") \
        .getOrCreate()

    schema = StructType([StructField("car", StringType(), True),
                         StructField("price", DoubleType(), True),
                         StructField("body", StringType(), True),
                         StructField("mileage", DoubleType(), True),
                         StructField("engV", StringType(), True),
                         StructField("engType", StringType(), True),
                         StructField("registration", StringType(), True),
                         StructField("year", IntegerType(), True),
                         StructField("model", StringType(), True),
                         StructField("drive", StringType(), True)])

    streams_df = spark.readStream \
        .option("header", True) \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .csv("datasets/carAdsDataset/dropLocation")

    agg_df = streams_df.groupBy("car") \
        .agg({'mileage': 'avg'}) \
        .withColumnRenamed("avg(mileage)", "average mileage")

    agg_df.writeStream\
        .outputMode("complete")\
        .format("console")\
        .trigger(processingTime='60 seconds')\
        .start()\
        .awaitTermination()