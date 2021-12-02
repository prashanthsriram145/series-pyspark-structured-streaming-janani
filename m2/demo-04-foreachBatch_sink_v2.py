import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local") \
                               .appName("Foreac batch sink") \
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

    def process_batch(df, epochId):

        print("Processing batch: ", epochId)

        dfFI = df.select("Date", "Country_Code", "Sold_Units").where("Country_Code == 'AT'")
        dfAI = df.select("Date", "Country_Code", "Sold_Units").where("Country_Code == 'AI'")
        dfFR = df.select("Date", "Country_Code", "Sold_Units").where("Country_Code == 'FR'")
        dfSE = df.select("Date", "Country_Code", "Sold_Units").where("Country_Code == 'SE'")

        file_path = os.path.join("foreachBatch_dir", str(epochId))

        dfFI.repartition(1) \
            .write.mode("append") \
            .csv(os.path.join("foreachBatch_dir", "AT"))

        dfAI.repartition(1) \
            .write.mode("append") \
            .csv(os.path.join("foreachBatch_dir", "AI"))

        dfFR.repartition(1) \
            .write.mode("append") \
            .csv(os.path.join("foreachBatch_dir", "FR"))

        dfSE.repartition(1) \
            .write.mode("append") \
            .csv(os.path.join("foreachBatch_dir", "SE"))


    query = fileStreamDf.writeStream \
                   .foreachBatch(process_batch) \
                   .outputMode("append") \
                   .start()

    query.awaitTermination()

 

