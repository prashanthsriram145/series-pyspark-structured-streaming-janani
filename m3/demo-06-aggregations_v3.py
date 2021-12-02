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

	aggregationDf = fileStreamDf.groupBy("year", "car")\
							    .agg({'price': 'avg'})\
							    .withColumnRenamed("avg(price)", "average_price")\
							    .orderBy("year", "car", ascending=False)


	query = aggregationDf.writeStream\
				     	 .outputMode("complete")\
				   	     .format("console")\
				   	     .option("numRows", 10)\
  					     .start()

	query.awaitTermination()

