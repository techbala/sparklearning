from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.functions import lit, expr, col


spark = SparkSession.builder.master("local").appName("basic_create").getOrCreate()

def schemaTest():
    schemaDf = spark.read.format('csv').option("header", "true").load("../../data/airlines.csv")
    print( schemaDf.schema )
    print( schemaDf.columns )
    print( schemaDf.dtypes )

def manualSchema():
    mSchema = StructType( [(StructField( "sno", LongType(), True )),
                                (StructField( "name", StringType(), True )),
                                (StructField( "age", LongType(), True ))])
    data = [[1,"bala", 38],[2, "krithish", 9 ], [3, "sachin", 6]]

    #data = [Row(1,"bala", 38),Row(2, "krithish", 9), Row(3, "sachin", 6])]

    df = spark.createDataFrame( data, mSchema )
    print( df.columns )
    print( df.dtypes )

def rddToDataFrame():
    mSchema = StructType([(StructField("sno", LongType(), True)),
                          (StructField("name", StringType(), True)),
                          (StructField("age", LongType(), True))])
    data = [[1, "bala", 38], [2, "krithish", 9], [3, "sachin", 6]]
    rddO = spark.sparkContext.parallelize( data )
    df = spark.createDataFrame( rddO, mSchema  )
    df.show(2)

def columnTest():
    columnDf = spark.read.format('csv').option("header", "true").load("../../data/airlines.csv")
    print( columnDf.columns[0] )
    #print( columnDf.selectExpr( "code" +lit(" ") + "description" ).take(5 ) )
    columnDf.selectExpr( "*", "(Code > 19040) as greaterValue").show(20)

def exprTest():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    flightsDf.selectExpr("*", "(departure + departure_delay) as totaltime " ).show(5)
    flightsDf.selectExpr( "avg(airlines)", "count(distinct(airlines))").show(5)
    flightsDf.select(expr('*'), lit(1).alias("one")).show(2)

def columnFunctions():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    flightsDf.withColumn("totaltime", expr("departure + departure_delay ") ).show(5)
    flightsDf.withColumnRenamed( "departure_delay", "ddelay").show(1)

def repartitionTest():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    print( flightsDf.rdd.getNumPartitions() )
    print( flightsDf.repartition(10).rdd.getNumPartitions() )
    print( flightsDf.repartition("airlines").rdd.getNumPartitions() )
    #flightsDf.repartition("airlines").foreachPartition( lambda e : print( e.from_iterable )  )

def filtersTest():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    flightsDf.filter( col('airlines') == 19805).show()
    flightsDf.where( col('airlines') == 19805).show()
    flightsDf.select( "airlines").distinct().show(5)

def dataframeTest():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    print(flightsDf.sample( withReplacement=False, fraction=0.2, seed=5 ).count())
    dataframes = flightsDf.randomSplit([0.2,0.8], seed=5 )
    print( dataframes[0].count( ) )
    print(dataframes[1].count())

def sortAndOrderByTest():
    flightsDf = spark.read.format('csv').option("header", "true").load("../../data/flights.csv")
    flightsDf.sort("airlines").show(5)
    flightsDf.orderBy( col('airlines').asc(), col('origin').desc() ).show(10)

#schemaTest()
#manualSchema()
#rddToDataFrame()
#columnTest()
#exprTest()
#columnFunctions()
repartitionTest()
#filtersTest()
#dataframeTest()
#sortAndOrderByTest()

