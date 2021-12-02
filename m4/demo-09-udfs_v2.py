import time
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, window



if __name__ == "__main__":

    sparkSession = SparkSession.builder.master("local")\
                               .appName("UserDefinedFunctions")\
                               .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("Invoice ID", StringType(), True),\
    					 StructField("Branch", StringType(), True),\
    					 StructField("City", StringType(), True),\
    					 StructField("Customer type", StringType(), True),\
    					 StructField("Gender", StringType(), True),\
    					 StructField("Product line", StringType(), True),\
    					 StructField("Unit price", DoubleType(), True),\
    					 StructField("Quantity", IntegerType(), True),\
    					 StructField("Tax 5%", DoubleType(), True),\
                         StructField("Total", DoubleType(), True),\
                         StructField("Date", StringType(), True),\
                         StructField("Time", StringType(), True),\
                         StructField("Payment", StringType(), True),\
                         StructField("cogs", DoubleType(), True),\
                         StructField("gross margin percentage", DoubleType(), True),\
                         StructField("gross income", DoubleType(), True),\
    					 StructField("Rating", DoubleType(), True)])


    fileStreamDf = sparkSession.readStream\
                               .option("header", "true")\
                               .schema(schema)\
                               .csv("datasets/supermarketSalesDataset/dropLocation")

    fileStreamDf.printSchema()

    def add_rating_status(rating):

        if rating >= 9:
            return 'Amazing'

        elif (rating >= 8):
            return 'Good'

        elif (rating >= 5):
            return 'Average'

        else:
            return 'Bad'

    sparkSession.udf.register("add_rating_status_udf", add_rating_status, StringType())

    fileStreamDf.createOrReplaceTempView('product_orders')

    ratingCategoryDf = sparkSession.sql("""SELECT `Invoice ID`, Rating, add_rating_status_udf(Rating) as Comments
                                           FROM product_orders
                                        """)

    query = ratingCategoryDf.writeStream\
                            .outputMode("append")\
                            .format("console")\
                            .option("truncate", "false")\
                            .start()\
                            .awaitTermination()




















