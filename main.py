#precisa ser feito antes to import do pyspark
from cProfile import label
from re import A
import findspark
findspark.init()
#-------------------------------------------#
import pyspark
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import sys, os, math, time
import seaborn as sns
import numpy as np
from tabulate import tabulate
from pyspark.sql import SparkSession,SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, LongType, IntegerType,FloatType,StringType

start_time = time.time()


spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "6g") \
    .appName('google-traces') \
    .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
instance_events_schema = StructType([ \
    StructField("time",LongType(),True), \
    StructField("type",IntegerType(),True), \
    StructField("collection_id",LongType(),False), \
    StructField("priority", IntegerType(), True), \
    StructField("instance_index", IntegerType(), False), \
    StructField("resource_request.cpus", FloatType(), True), \
    StructField("resource_request.memory", FloatType(), True) \
  ])
collection_events_schema = StructType([ \
  StructField("time",LongType(),True), \
  StructField("type",IntegerType(),True), \
  StructField("collection_id",LongType(),False), \
  StructField("priority", IntegerType(), True) \
])

def microsToHour(x):
    return math.floor(x/3.6e+9)

def labelCategory(valuePriority):
    if valuePriority<100:
        return "Free-tier"
    elif valuePriority>=100 and valuePriority<=115:
        return "Best-Efort-Batch"
    elif valuePriority>115 and valuePriority<120:
        return "Mid-Tier"
    elif valuePriority>=120 and valuePriority<360:
        return "Production-tier"
    else:
        return "Monitoring-tier"
labelCategory(400)
labelCategory(200)
#Define a função no formato correto para ser aplicada ao RDD
udf_hours = F.udf(lambda x:microsToHour(x),IntegerType())
udf_cats = F.udf(lambda x:labelCategory(x),StringType())

#variáveis utilizadas nas análises
minTimeSubmitTask = sys.maxsize
maxTimeSubmitTask = 0
submitTaskCount = 0
rddResources = sc.emptyRDD()
rddJobsAndTaskTimes = sc.emptyRDD()
rddResByPriority = sc.emptyRDD()
filesCounter=1
a = []
timefA = time.time()

#coleta as métricas de collection_events
rddCE = spark.read.option("header","true").schema(collection_events_schema).csv("collection_events/collection_events-000000000000.csv")
rddCE = rddCE.filter((rddCE.time.isNotNull()) & (rddCE.time>0) & (rddCE.time<sys.maxsize))
rddJobsSubPerHour = rddCE.filter(rddCE.type==0)
rddMinJobTimeByCID = rddCE.groupBy("collection_id")\
    .agg({"time":"min"}).withColumnRenamed("min(time)","time_jobs")

#percorre os instance_events, coletando métricas utilizadas nas análises
for filename in os.listdir("instance_events"):
  
  if filesCounter==50:
    break
  f = os.path.join("instance_events",filename)
  rddIE = spark.read.option("header","true").schema(instance_events_schema).csv(f)
  if rddIE.isEmpty():
    continue
  rddResByPriorityCur = rddIE.select("collection_id","`resource_request.cpus`","`resource_request.memory`")
  rddResByPriorityCur = rddCE.select("collection_id","priority")\
      .join(rddResByPriorityCur,rddResByPriorityCur.collection_id==rddCE.collection_id).drop(rddResByPriorityCur.collection_id)
  rddResByPriorityCur = rddResByPriorityCur.groupBy("priority").agg(F.sum("`resource_request.cpus`"),F.sum("`resource_request.memory`"),F.count("priority"))

  rddIE = rddIE.filter((rddIE.time.isNotNull()) & (rddIE.time>0) & (rddIE.time<sys.maxsize)) 

  rddResourcesCurrent =  rddIE.withColumn("hour",udf_hours(col("time")))\
    .groupBy('hour').agg(F.sum("`resource_request.cpus`"),F.sum("`resource_request.memory`"))

     
  rddTasksSubPerHour = rddIE.filter(rddIE.type==0)

  rddminTaskTimeByCID = rddIE.filter(rddIE.type==3).groupBy("collection_id")\
    .agg({"time":"min"}).withColumnRenamed("min(time)","time_tasks")

  rddJobsAndTaskTimesCur = rddminTaskTimeByCID\
      .join(rddMinJobTimeByCID,rddminTaskTimeByCID.collection_id==rddMinJobTimeByCID.collection_id)\
      .drop(rddMinJobTimeByCID.collection_id)
  
  if(filesCounter==1):
    rddResources = rddResourcesCurrent
    rddResByPriority = rddResByPriorityCur
    rddJobsAndTaskTimes=rddJobsAndTaskTimesCur
  else:
    rddJobsAndTaskTimes=rddJobsAndTaskTimes.union(rddJobsAndTaskTimesCur)
    
    rddResByPriority =  rddResByPriority.withColumnRenamed("sum(`resource_request.memory`)","m_sum1")\
      .withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum1")\
        .withColumnRenamed("count(priority)","p_count1")
    rddResByPriorityCur=  rddResByPriorityCur.withColumnRenamed("sum(`resource_request.memory`)","m_sum2")\
      .withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum2")\
        .withColumnRenamed("count(priority)","p_count2")
    rddResByPriority = rddResByPriority.union(rddResByPriorityCur)
    rddResByPriority = rddResByPriority.join(rddResByPriorityCur,rddResByPriority.priority == rddResByPriorityCur.priority)\
      .withColumn("m_sum1",col("m_sum1")+col("m_sum2")).drop("m_sum2")\
        .withColumn("cpus_sum1",col("cpus_sum1")+col("cpus_sum2")).drop("cpus_sum2")\
          .withColumn("p_count1",col("p_count1")+col("p_count2")).drop("p_count2")\
            .drop(rddResByPriority.priority)
    
    rddResources =  rddResources.withColumnRenamed("sum(`resource_request.memory`)","m_sum1")\
      .withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum1")
    rddResourcesCurrent=  rddResourcesCurrent.withColumnRenamed("sum(`resource_request.memory`)","m_sum2")\
      .withColumnRenamed("sum(`resource_request.cpus`)","cpus_sum2")

    rddResources = rddResources.join(rddResourcesCurrent,rddResources.hour == rddResourcesCurrent.hour).drop(rddResourcesCurrent.hour)\
      .withColumn("m_sum1",col("m_sum1")+col("m_sum2")).drop("m_sum2")\
        .withColumn("cpus_sum1",col("cpus_sum1")+col("cpus_sum2")).drop("cpus_sum2")
  
  taskCountMetrics = rddTasksSubPerHour.agg(F.count("time"),F.min("time"),F.max("time")).collect()[0]
  taskCountCurr,minTimeTaskCur,MaxTimeTaskCur = taskCountMetrics[0],taskCountMetrics[1],taskCountMetrics[2] 
  submitTaskCount = submitTaskCount+taskCountCurr
  if taskCountCurr>0:
      maxTimeSubmitTask = max(maxTimeSubmitTask,MaxTimeTaskCur)
      minTimeSubmitTask = min(minTimeSubmitTask,minTimeTaskCur)
  filesCounter+=1
  
timefB= time.time()
jobCountMetrics = rddJobsSubPerHour.agg(F.count("time"),F.min("time"),F.max("time")).collect()[0]
jobCount,minTimeSubmitJob,maxTimeSubmitJob = jobCountMetrics[0],jobCountMetrics[1],jobCountMetrics[2]
avgStartTimes = rddJobsAndTaskTimes.agg(F.avg("time_tasks"),F.avg("time_jobs")).collect()[0]

atime = time.time()
resourcesToPlot = [(val.m_sum1,val.cpus_sum1,val.hour) for val in \
  rddResources.select('m_sum1','cpus_sum1','hour').orderBy('hour').collect()]
y_sum_memory,y_sum_cpus,x_time_hours = \
  [x[0] for x in resourcesToPlot],[x[1] for x in resourcesToPlot],[x[2] for x in resourcesToPlot]
btime = time.time()
timeResCatA = time.time()
rddResByPriority = rddResByPriority.withColumn("avg_cpus_rr",col("cpus_sum1")/col("p_count1"))\
  .withColumn("avg_mem_rr",col("m_sum1")/col("p_count1"))\
    .select("priority","avg_cpus_rr","avg_mem_rr")
resPerCategory = rddResByPriority.withColumn("categoria",udf_cats(col("priority")))\
  .groupBy("categoria").agg(F.avg("avg_cpus_rr"),F.avg("avg_mem_rr")).collect()
timeResCatB = time.time()

plt.plot(x_time_hours, y_sum_memory,label="memória")

plt.ylabel('Requisição de memória')
plt.xlabel('tempo(hora desde o início da execução)')
plt.title('Requisição de memória ao longo do tempo')
plt.savefig('consumomemoria.png',dpi=200)
plt.clf()
plt.plot(x_time_hours, y_sum_cpus,label="cpus")
plt.ylabel('Requisição de cpus')
plt.xlabel('tempo(hora desde o início da execução)')
plt.title('Requisição de cpus ao longo do tempo')
plt.savefig('consumocpus.png',dpi=200)
plt.clf()
plt.figure(figsize = (10, 6), dpi = 100)
sns.barplot(x=[j[0] for j in resPerCategory],y=[j[1] for j in resPerCategory])
plt.savefig("usoCpuPorCat.png").set(title="Requisição de cpus por categoria de Task")
plt.clf()
plt.figure(figsize = (10, 6), dpi = 100)
sns.barplot(x=[j[0] for j in resPerCategory],y=[j[2] for j in resPerCategory])
plt.savefig("usoMemPorCat.png").set(title="Requisição de memória por categoria de Task")
plt.clf()
plt.figure(figsize = (10, 6), dpi = 100)
sns.regplot(x=y_sum_cpus,y=y_sum_memory).set(title="Relação entre requisições de cpus e memória")
plt.savefig("relacaoMemoriaCpus.png")

outputFile = open("métricas.txt",'w')
outputFile.write("Requisicao de recursos por categoria:\n")
outputFile.write(tabulate(resPerCategory,headers=["Categoria","Requisicao media de cpus","Requisicao media de memoria"])+"\n\n")
outputFile.write("Jobs submetidas por hora, em media: "+str(jobCount/(microsToHour(maxTimeSubmitJob)-microsToHour(minTimeSubmitJob)))+"\n")
outputFile.write("Tasks submetidas por hora, em media: "+str(submitTaskCount/(microsToHour(maxTimeSubmitTask)-microsToHour(minTimeSubmitTask)))+"\n")
outputFile.write("Tempo medio delay de inicio entre jobs e tasks:(ms) "+str((avgStartTimes[1]-avgStartTimes[0])/1000)+"\n")
outputFile.write("Coeficiente de correlacao entre requisicao de memoria e cpu: "+str(np.corrcoef(y_sum_cpus,y_sum_memory)[0][1])+"\n")

outputFile.write("Tempo de execucao(s): "+str(time.time()-start_time)+"\n\n")
outputFile.write("Tempo de carregamento variaveis do plot: "+str(btime-atime)+"\n")
outputFile.write("Tempo de preenchimento por categoria: "+str(timeResCatB-timeResCatA)+"\n")
outputFile.write("Tempo dos fors: "+str(timefB-timefA)+"\n")
outputFile.close()



