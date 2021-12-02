from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local") \
            .appName("file-sink").getOrCreate()

    schema = StructType([StructField("Date", StringType(), False),
                         StructField("Article_ID", StringType(), False),
                         StructField("Country_Code", StringType(), False),
                         StructField("Sold_Units", IntegerType(), False)])

    df = spark.readStream \
        .option("header", True) \
        .schema(schema) \
        .csv("datasets/historicalDataset/dropLocation")

    df = df.select("*")

    df = df.writeStream.format("json") \
        .option("path", "file-sink-output") \
        .outputMode("append") \
        .option("checkpointLocation", "file-sink-checkpoint") \
        .start()

    df.awaitTermination()