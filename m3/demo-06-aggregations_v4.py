from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

	sparkSession = SparkSession.builder.master("local")\
                               .appName("Aggregations")\
                               .getOrCreate()

	sparkSession.sparkContext.setLogLevel("ERROR")


	schema = StructType([StructField("car", StringType(), True),\
    					 StructField("price", DoubleType(), True),\
    					 StructField("body", StringType(), True),\
    					 StructField("mileage", DoubleType(), True),\
    					 StructField("engV", StringType(), True),\
    					 StructField("engType", StringType(), True),\
    					 StructField("registration", StringType(), True),\
    					 StructField("year", IntegerType(), True),\
    					 StructField("model", StringType(), True),\
    					 StructField("drive", StringType(), True)])


	fileStreamDf = sparkSession.readStream\
					 .option("header", "true")\
					 .schema(schema)\
					 .csv("datasets/carAdsDataset/droplocation")


	fileStreamDf.printSchema()

	fileStreamDf = fileStreamDf.select("*")\
						.where(fileStreamDf.price != 0)

	fileStreamDf.createOrReplaceTempView("car_ads")
	
	aggregationDf = sparkSession.sql(""" SELECT car as make, avg(price), min(price), max(price)
										 FROM car_ads
										 WHERE year > 2013
										 GROUP BY make
									""")


	query = aggregationDf.writeStream\
				     	 .outputMode("complete")\
				   	     .format("console")\
				   	     .option("numRows", 10)\
  					     .start()

	query.awaitTermination()

