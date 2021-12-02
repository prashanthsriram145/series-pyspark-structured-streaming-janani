import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local") \
                               .appName("Foreach batch sink") \
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

    def process_batch(df, epochId):

        print(df, epochId)
        df.show()

        file_path = os.path.join("foreachBatch_dir", str(epochId))

        q1 = df.repartition(1) \
                    .write.mode("errorifexists") \
                    .csv(file_path)



    query = countDf.writeStream \
                   .foreachBatch(process_batch) \
                   .outputMode("update") \
                   .start()

    query.awaitTermination()

 

