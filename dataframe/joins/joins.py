from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, approx_count_distinct, first, last, expr, max, col, rank, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.master('local').appName('aggregationBasic').getOrCreate()
flights = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("../../data/flights.csv")
airports = spark.read.format('csv').option("header", "true").option("inferSchema", "true").load(
    "../../data/airports.csv")


def innerJoinTest():
    flights.join( airports, flights.origin == airports.Code ).show()
    flights.join(airports, flights["origin"] == airports["Code"],  "inner").show()

def outerJoinTest():
    flights.join(airports, flights["origin"] == airports["Code"], "outer").show(50)

def leftOuterJoinTest():
    flights.join(airports, flights["origin"] == airports["Code"], "left_outer").show(50)

def rightOuterJoinTest():
    flights.join(airports, flights["origin"] == airports["Code"], "right_outer").show(50)

def leftSemiJoinTest():
    flights.join(airports, flights["origin"] == airports["Code"], "left_semi").show(50)

def leftSemiJoinFurtherTest():
    a = spark.createDataFrame( [[1],[1],[2], [3]])
    b = spark.createDataFrame( [[1],[2],[2], [4] ])
    joinExpression = a['_1'] == b['_1']
    print( "--------------------inner------------------")
    a.join(b, joinExpression, how='inner').show()
    print("--------------------outer------------------")
    a.join(b, joinExpression,how='outer').show()
    print("--------------------left_outer------------------")
    a.join(b, joinExpression,how='left_outer').show()
    print("--------------------right_outer------------------")
    a.join(b, joinExpression,how='right_outer').show()
    print("--------------------left_semi------------------")
    a.join(b, joinExpression,how='left_semi').show()
    print("--------------------left_anti------------------")
    a.join(b, joinExpression,how='left_anti').show()
    print("--------------------cross------------------")
    a.join(b, joinExpression,how='cross').show()

leftSemiJoinFurtherTest()