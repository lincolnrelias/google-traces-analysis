#precisa ser feito antes to import do pyspark
from sqlite3 import TimeFromTicks
import findspark
findspark.init()
#-------------------------------------------#
import pyspark
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import sys, os, math, time
from pyspark.sql import SparkSession,SQLContext
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, LongType, IntegerType,FloatType

start_time = time.time()


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
collection_events_schema = StructType([ \
  StructField("time",LongType(),True), \
  StructField("type",IntegerType(),True), \
  StructField("collection_id",IntegerType(),False), \
  StructField("priority", IntegerType(), True) \
])
resSchema = StructType([ \
    StructField("hour",IntegerType(),True), \
    StructField("m_sum1", FloatType(), True), \
    StructField("cpus_sum1", FloatType(), True) \
  ])

def microsToHour(x):
    return math.floor(x/3.6e+9)

#Define a função no formato correto para ser aplicada ao RDD
udf_hours = F.udf(lambda x:microsToHour(x),IntegerType())

#variáveis utilizadas nas análises
minTimeSubmitTask = sys.maxsize
maxTimeSubmitTask = 0
submitTaskCount = 0
rddResources = sc.emptyRDD()
filesToRunBeforeStopping=1
timefA = time.time()
#percorre os instance_events, coletando métricas utilizadas nas análises
for filename in os.listdir("instance_events"):
  
  if filesToRunBeforeStopping==3:
    break
  f = os.path.join("instance_events",filename)
  rddRaw = spark.read.option("header","true").schema(instance_events_schema).csv(f)
    
  rddResourcesCurrent =  rddRaw.filter((rddRaw.time.isNotNull()) & (rddRaw.time>0) & (rddRaw.time<sys.maxsize))
  rddTasksSubPerHour = rddRaw.filter((rddRaw.time.isNotNull()) & (rddRaw.type==0) & (rddRaw.time>0) & (rddRaw.time<sys.maxsize))

  rddResourcesCurrent = rddResourcesCurrent.withColumn("hour",udf_hours(col("time")))
  rddResourcesCurrent = rddResourcesCurrent.groupBy('hour').agg(F.sum("`resource_request.cpus`"),F.sum("`resource_request.memory`"))
  if(filesToRunBeforeStopping==1):
    rddResources = rddResourcesCurrent
  else:
    rddResources =  rddResources.withColumnRenamed("sum(`resource_request.memory`)","m_sum1").\
      withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum1")
    rddResourcesCurrent=  rddResourcesCurrent.withColumnRenamed("sum(`resource_request.memory`)","m_sum2").\
      withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum2")

    rddResources = rddResources.join(rddResourcesCurrent,rddResources.hour == rddResourcesCurrent.hour).drop(rddResourcesCurrent.hour)\
      .withColumn("m_sum1",col("m_sum1")+col("m_sum2")).drop("m_sum2")\
        .withColumn("cpus_sum1",col("cpus_sum1")+col("cpus_sum2")).drop("cpus_sum2")
    
  taskCountCurr = rddTasksSubPerHour.agg({'time':'count'}).collect()[0][0]  
  submitTaskCount = submitTaskCount+taskCountCurr
  if taskCountCurr>0:
      maxTimeSubmitTask = max(maxTimeSubmitTask,rddTasksSubPerHour.agg({'time':'max'}).collect()[0][0])
      minTimeSubmitTask = min(minTimeSubmitTask,rddTasksSubPerHour.agg({'time':'min'}).collect()[0][0])
  filesToRunBeforeStopping+=1
  
#coleta as métricas de collection_events
rddJobsSubPerHour = spark.read.option("header","true").schema(collection_events_schema).csv("collection_events/collection_events-000000000000.csv")
rddJobsSubPerHour = rddJobsSubPerHour.filter((rddJobsSubPerHour.time.isNotNull()) & (rddJobsSubPerHour.type==0) & (rddJobsSubPerHour.time>0) & (rddJobsSubPerHour.time<sys.maxsize))
maxTimeSubmitJob = rddJobsSubPerHour.agg({'time':'max'}).collect()[0][0]
minTimeSubmitJob = rddJobsSubPerHour.agg({'time':'min'}).collect()[0][0]
jobCount = rddJobsSubPerHour.agg({'time':'count'}).collect()[0][0]  
timefB= time.time()
atime = time.time()
rddResources.persist()
y_sum_memory = [val.m_sum1 for val in rddResources.select('m_sum1').orderBy('hour').collect()]
y_sum_cpus = [val.cpus_sum1 for val in rddResources.select('cpus_sum1').orderBy('hour').collect()]
x_time_hours = [val.hour for val in rddResources.select('hour').orderBy('hour').collect()]
btime = time.time()
plt.plot(x_time_hours, y_sum_memory,label="memória")
plt.plot(x_time_hours, y_sum_cpus,label="cpus")
plt.ylabel('consumo de memória')
plt.xlabel('tempo(hora desde o início da execução)')
plt.title('Consumo de memória ao longo do tempo')
plt.savefig('temp1.png',dpi=200)
outputFile = open("métricas.txt",'w')
outputFile.write("Tempo minimo de submissao de task(ms): "+str(minTimeSubmitTask)+"\n")
outputFile.write("Tempo maximo de submissao de task(ms): "+str(maxTimeSubmitTask)+"\n")
outputFile.write("Tempo minimo de submissao de jobs(ms): "+str(minTimeSubmitJob)+"\n")
outputFile.write("Tempo maximo de submissao de jobs(ms): "+str(maxTimeSubmitJob)+"\n")
outputFile.write("Jobs submetidas por hora: "+str(jobCount/(microsToHour(maxTimeSubmitJob)-microsToHour(minTimeSubmitJob)))+"\n")
outputFile.write("Tasks submetidas por hora: "+str(submitTaskCount/(microsToHour(maxTimeSubmitTask)-microsToHour(minTimeSubmitTask)))+"\n")
outputFile.write("Tempo de execucao(s): "+str(time.time()-start_time)+"\n")
outputFile.write("Tempo de carregamento variaveis do plot: "+str(btime-atime)+"\n")
outputFile.write("Tempo dos fors: "+str(timefB-timefA)+"\n")
outputFile.close()



