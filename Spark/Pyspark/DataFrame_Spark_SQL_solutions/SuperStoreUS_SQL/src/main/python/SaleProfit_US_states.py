import sys
import configparser as config
import datetime

from pyspark.sql import SparkSession

props = config.RawConfigParser()
props.read("src/main/configs/app.properties")
exMode = sys.argv[1]

spark = SparkSession.\
    builder.\
    master(props.get(exMode, 'deployMode')).\
    appName("Sale Profit Per State in US").\
    getOrCreate()

sc = spark.sparkContext

# Handling with file system
dataInPath = props.get(exMode, 'data.in.path')
dataOutPath = props.get(exMode, 'data.out.path')

# HDFS API's
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSytem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

fs = FileSytem.get(Configuration())

if (fs.exists(Path(dataInPath)) == True):
    if (fs.exists(Path(dataOutPath))):
        dataOutPath = dataOutPath + "_" + str(datetime.datetime.now().strftime("%y_%m_%d_%H_%M"))
        print("New Path = ", dataOutPath)

    dataDF = spark.\
        read.\
        csv(path=dataInPath, sep=",", header=True, inferSchema=True)

    dataDF.registerTempTable("SuperStore")
    dataAggDF = spark.\
        sql("select Country, State, round(sum(cast(Profit AS double)), 2) as Total_profit from SuperStore group by Country, State order by Total_profit desc")

    dataAggDF.coalesce(2).write.csv(dataOutPath)

else:
    print("Input Path is not exist" + dataInPath)

spark.stop()



