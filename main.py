#precisa ser feito antes to import do pyspark
from xmlrpc.client import MAXINT
import findspark
findspark.init()
#-------------------------------------------#
import pyspark
from pyspark_dist_explore import hist
import time
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import math
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField, LongType, IntegerType,FloatType
spark = SparkSession.builder.appName("Sessao").getOrCreate()
sc = spark.sparkContext
instance_events_schema = StructType([ \
    StructField("time",LongType(),True), \
    StructField("type",IntegerType(),True), \
    StructField("collection_id",IntegerType(),False), \
    StructField("priority", IntegerType(), True), \
    StructField("instance_index", IntegerType(), False), \
    StructField("resource_request.cpus", FloatType(), True), \
    StructField("resource_request.memory", FloatType(), True) \
  ])
#carrega o csv em um RDD, sem cabeçalho
rddCE = spark.read.option("header","true").schema(instance_events_schema).csv("instance_events/instance_events-000000000000.csv")
#print(rddCE.take(5))
#seleciona as colunas tipo e prioridade
#rddCE.select([*['type','priority']]).show()

#fig, ax = plt.subplots()
#test = hist(ax, rddCE.select('type'), bins = 20, color=['red'])
rddCE.persist()
#rddCE.orderBy('time').groupBy('time').agg({"`memory_resource_request`":'avg'}).show()

rddCE = rddCE.filter((rddCE.time.isNotNull()) & (rddCE.type==3) & (rddCE.time>0) & (rddCE.time<sys.maxsize))
def microToHour(x):
    return math.floor(x/3.6e+9)
rddCE.take(5)
rddCE.withColumn("hour",int(col("time"))).select("time","hour").distinct().show(5)
udf_hours = udf(lambda x:microToHour(x),IntegerType())

rddCE.withColumn("hour",udf_hours(col("time"))).show(10)
rddCE.withColumn("hour",udf_hours(col("time"))).select('time','hour').agg({'time','max'}).collect[0]