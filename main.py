#precisa ser feito antes to import do pyspark
import findspark
findspark.init()
#-------------------------------------------#
import pyspark
from pyspark_dist_explore import hist
import time
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,FloatType
spark = SparkSession.builder.appName("Sessao").getOrCreate()
sc = spark.sparkContext
instance_events_schema = StructType([ \
    StructField("time",IntegerType(),True), \
    StructField("type",IntegerType(),True), \
    StructField("collection_id",IntegerType(),False), \
    StructField("priority", IntegerType(), True), \
    StructField("instance_index", IntegerType(), False), \
    StructField("cpu_resource_request", FloatType(), True), \
    StructField("memory_resource_request", FloatType(), True) \
  ])
#carrega o csv em um RDD, sem cabeçalho
rddCE = spark.read.option("header","true").schema(instance_events_schema).csv("instance_events/instance_events-000000000000.csv")
#print(rddCE.take(5))
#seleciona as colunas tipo e prioridade
#rddCE.select([*['type','priority']]).show()

#fig, ax = plt.subplots()
#test = hist(ax, rddCE.select('type'), bins = 20, color=['red'])
rddCE.orderBy('type').groupBy('type').agg({"`memory_resource_request`":'avg'}).show()