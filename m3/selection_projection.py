from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local") \
        .appName("streaming and projection") \
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

    stream_df = spark.readStream \
        .option("header", True) \
        .schema(schema) \
        .csv("datasets/carAdsDataset/dropLocation")

    projections_df = stream_df.select("car", "price", "mileage", "year", "model") \
        .where("year == 2010")

    projections_df.printSchema()

    projections_df.writeStream\
        .outputMode("append")\
        .format("console")\
        .start()\
        .awaitTermination()

