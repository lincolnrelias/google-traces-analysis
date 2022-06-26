#precisa ser feito antes to import do pyspark
import findspark
findspark.init()
#-------------------------------------------#
import pyspark
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import sys, os, math, time
from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField, LongType, IntegerType,FloatType

spark = SparkSession.builder.appName("Sessao").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
instance_events_schema = StructType([ \
    StructField("time",LongType(),True), \
    StructField("type",IntegerType(),True), \
    StructField("collection_id",IntegerType(),False), \
    StructField("priority", IntegerType(), True), \
    StructField("instance_index", IntegerType(), False), \
    StructField("resource_request.cpus", FloatType(), True), \
    StructField("resource_request.memory", FloatType(), True) \
  ])
memSumSchema = StructType([ \
    StructField("hour",IntegerType(),True), \
    StructField("m_sum1", FloatType(), True) \
  ])

def microsToHour(x):
    return math.floor(x/3.6e+9)

#Define a função no formato correto para ser aplicada ao RDD
udf_hours = udf(lambda x:microsToHour(x),IntegerType())

#variáveis utilizadas nas análises
minTimeSubmitTask = sys.maxsize
maxTimeSubmitTask = 0
submitTaskCount = 0
rddMemRR = sc.emptyRDD()
filesToRunBeforeStopping=0
#percorre os instance_events, coletando métricas utilizadas nas análises
for filename in os.listdir("instance_events"):
    if filesToRunBeforeStopping==5:
      break
    f = os.path.join("instance_events",filename)
    rddRaw = spark.read.option("header","true").schema(instance_events_schema).csv(f)
    
    rddMemRRCurrent =  rddRaw.filter((rddRaw.time.isNotNull()) & (rddRaw.time>0) & (rddRaw.time<sys.maxsize))
    rddSubsPerHour = rddRaw.filter((rddRaw.time.isNotNull()) & (rddRaw.type==3) & (rddRaw.time>0) & (rddRaw.time<sys.maxsize))

    rddMemRRCurrent = rddMemRRCurrent.withColumn("hour",udf_hours(col("time")))
    rddMemRRCurrent = rddMemRRCurrent.groupBy('hour').agg({"`resource_request.memory`":'sum'})
    if(i==0):
      rddMemRR = rddMemRRCurrent
    else:
      rddMemRR =  rddMemRR.withColumnRenamed("sum(resource_request.memory)","m_sum1")
      rddMemRRCurrent=  rddMemRRCurrent.withColumnRenamed("sum(resource_request.memory)","m_sum2")

      rddMemRR = rddMemRR.join(rddMemRRCurrent,rddMemRR.hour == rddMemRRCurrent.hour).drop(rddMemRRCurrent.hour)\
        .withColumn("m_sum1",col("m_sum1")+col("m_sum2")).drop("m_sum2")
      
    submitTaskCount = submitTaskCount+rddSubsPerHour.agg({'time':'count'}).collect()[0][0]
    if rddSubsPerHour.agg({'time':'count'}).collect()[0][0]>0:
        maxTimeSubmitTask = max(maxTimeSubmitTask,rddSubsPerHour.agg({'time':'max'}).collect()[0][0])
        minTimeSubmitTask = min(minTimeSubmitTask,rddSubsPerHour.agg({'time':'min'}).collect()[0][0])
    filesToRunBeforeStopping+=1

y_sum_memory = [val.m_sum1 for val in rddMemRR.select('m_sum1').orderBy('hour').collect()]
x_time_hours = [val.hour for val in rddMemRR.select('hour').orderBy('hour').collect()]
plt.plot(x_time_hours, y_sum_memory)

plt.ylabel('consumo de memória')
plt.xlabel('tempo(hora desde o início da execução)')
plt.title('Consumo de memória ao longo do tempo')

plt.show()
plt.figure().savefig('temp.png')
print(str(minTimeSubmitTask)+", "+str(maxTimeSubmitTask)+", "+str(submitTaskCount/(microsToHour(maxTimeSubmitTask)-microsToHour(minTimeSubmitTask))))



