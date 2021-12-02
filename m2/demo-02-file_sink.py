from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

	sparkSession = SparkSession.builder.master("local") \
                               .appName("File sink") \
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

	selectedDf = fileStreamDf.select("*")

	projectionsDf = selectedDf.select(selectedDf.Date,
									  selectedDf.Country_Code,
									  selectedDf.Sold_Units) \
							   .withColumnRenamed("Date", "date") \
							   .withColumnRenamed("Country_Code", "countryCode") \
							   .withColumnRenamed("Sold_Units", "soldUnits")	


	query = projectionsDf.writeStream \
				   	     .outputMode("append") \
				   	     .format("json") \
				   	     .option("path", "filesink_results") \
					   	 .option("checkpointLocation", 'filesink_checkpoint') \
					     .start()

	query.awaitTermination()
