import os.path

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local") \
        .appName("foreach_sink") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    schema = StructType([StructField("Date", StringType(), False),
                         StructField("Article_ID", StringType(), False),
                         StructField("Country_Code", StringType(), False),
                         StructField("Sold_Units", IntegerType(), False)])

    spark_df = spark.readStream \
        .option("header", True) \
        .schema(schema) \
        .csv("datasets/historicalDataset/dropLocation")

    counts_df = spark_df.groupBy("Country_Code").count()

    foreach_dir = "foreach_dir"

    def process(row):
        print(row)
        print("-----------------")
        print(row["Country_Code"], row["count"])

        file_name = os.path.join(foreach_dir, row["Country_Code"])

        with open(file_name, 'w') as f:
            f.write("%s, %s" % (row["Country_Code"], row["count"]))
            f.close()


    counts_df.printSchema()

    counts_df.writeStream\
        .foreach(process)\
        .outputMode("complete")\
        .start()\
        .awaitTermination()


