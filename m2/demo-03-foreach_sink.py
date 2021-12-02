import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local") \
                               .appName("Foreach sink") \
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    schema = StructType([StructField("Date", StringType(), False), \
                         StructField("Article_ID", StringType(), False), \
                         StructField("Country_Code", StringType(), False), \
                         StructField("Sold_Units", IntegerType(), False)])


    fileStreamDf = sparkSession.readStream \
                               .option("header", "true") \
                               .schema(schema) \
                               .csv("datasets/historicalDataset/dropLocation")

    print(" ")
    print("Is the stream ready? ", fileStreamDf.isStreaming)

    print(" ")
    print("Stream schema ", fileStreamDf.printSchema())

    countDf = fileStreamDf.groupBy(fileStreamDf.Country_Code).count()

    process_dir = "foreach_dir"

    def process_row(row):

        print("------------")
        print(row)

        file_path = os.path.join(process_dir, row["Country_Code"])

        with open(file_path, 'w') as f:
            f.write("%s, %s\n" % (row["Country_Code"],  row["count"]))
            f.close()


    query = countDf.writeStream \
                   .foreach(process_row) \
                   .outputMode("complete") \
                   .start() \
                   .awaitTermination()

 

