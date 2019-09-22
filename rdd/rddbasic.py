from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, first, last, expr, max, col, rank, dense_rank

spark = SparkSession.builder.master('local').appName('aggregationBasic').getOrCreate()

def countTest():
    countDf = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../data/flights.csv")
    print( countDf.count() )
    print( countDf.rdd.getNumPartitions() )
    print(countDf.repartition(10).rdd.pipe("wc -l").collect())

countTest()