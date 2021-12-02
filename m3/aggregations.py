from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local")\
        .appName("aggregations")\
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

    stream_df = spark.readStream\
        .option("header", True)\
        .option("maxFilesPerTrigger", 1)\
        .schema(schema)\
        .csv("datasets/carAdsDataset/dropLocation")

    agg_df = stream_df.groupBy("car")\
        .count()\
        .withColumnRenamed("count", "car count")

    agg_df.writeStream\
        .outputMode("update")\
        .format("console")\
        .start()\
        .awaitTermination()
