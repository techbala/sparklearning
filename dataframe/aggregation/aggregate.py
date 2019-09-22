from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, first, last, expr, max, col, rank, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local').appName('aggregationBasic').getOrCreate()

def countTest():
    countDf = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../../data/airlines.csv")
    print( countDf.count() )
    countDf.select(count('Code')).show()
    countDf.select( countDistinct('Code') ).show()
    countDf.select( first('Code'), last('Code')).show()

def coalesceTest():
    countDf = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../../data/airlines.csv")
    print( countDf.rdd.getNumPartitions() )
    print(countDf.coalesce(5).rdd.getNumPartitions())

def groupByTest():
    df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../../data/flights.csv")
    df.groupBy("flight_number", "origin").count().show()
    df.groupBy("flight_number").agg( count("origin"), expr( "count(origin)") ).show()

def maxDepartureDelay():
    windowSpec = Window.partitionBy( "date", "origin").orderBy(col("departure_delay").desc() ).rowsBetween( Window.unboundedPreceding, Window.currentRow )
    rankOver = rank().over( windowSpec )
    denseOver = dense_rank().over( windowSpec )
    df = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../../data/flights.csv")
    df.select( "date", "origin", "departure_delay", rankOver.alias('rankOver'), denseOver.alias("denseOver") ).show()


#countTest()
#coalesceTest()
#groupByTest()
maxDepartureDelay()