from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local")\
        .appName("windowing")\
        .getOrCreate()

    schema = StructType([StructField("Date", TimestampType(), True),
                         StructField("Open", DoubleType(), True),
                         StructField("High", DoubleType(), True),
                         StructField("Low", DoubleType(), True),
                         StructField("Close", DoubleType(), True),
                         StructField("Adjusted Close", DoubleType(), True),
                         StructField("Volume", DoubleType(), True),
                         StructField("Name", StringType(), True)
                         ])

    stream_df = spark.readStream\
        .option("header", True)\
        .schema(schema)\
        .option("maxFilesPerTrigger", 1)\
        .csv("datasets/stockPricesDataset/dropLocation")

    agg_df = stream_df.withWatermark("Date", '10 minutes')\
        .groupBy(window(stream_df.Date, '7 days', '3 days'), stream_df.Name)\
        .agg({'High': 'avg'})\
        .withColumnRenamed("avg(High)", "average high")

    agg_df.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", False)\
        .start()\
        .awaitTermination()
4