from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local")\
        .appName("static_streaming_join")\
        .getOrCreate()

    static_schema = StructType([StructField("Country", StringType(), True),\
                                StructField("Year", StringType(), True),\
                                StructField("GDP", DoubleType(), True),\
                                StructField("Population", DoubleType(), True)])

    static_df = spark.read\
        .format("csv")\
        .option("header", True)\
        .schema(static_schema)\
        .load("datasets/lifeExpectancyDataset/static_files/*.csv")

    streaming_schema = StructType([StructField("Country", StringType(), True),\
                                   StructField("Year", StringType(), True),\
                                   StructField("Status", StringType(), True),\
                                   StructField("LifeExpectancy", DoubleType(), True)])

    stream_df = spark.readStream\
        .option("header", True)\
        .schema(streaming_schema)\
        .csv("datasets/lifeExpectancyDataset/dropLocation_1")

    joined_df = static_df.join(stream_df, on=["Country", "Year"])

    selected_df = joined_df.select("Country", "Year", "GDP", "LifeExpectancy")

    selected_df.writeStream\
        .outputMode("update")\
        .format("console")\
        .start()\
        .awaitTermination()



