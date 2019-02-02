import sys
import configparser as config
import datetime

from pyspark import SparkConf, SparkContext

props = config.RawConfigParser()
props.read("src/main/configs/app.properties")
exMode = sys.argv[1]

conf = SparkConf().\
    setMaster(props.get(exMode, 'deployMode')).\
    setAppName("Word Count")
sc = SparkContext(conf= conf)

#Handling with file system
dataInPath = props.get(exMode, 'data.in.path')
dataOutPath = props.get(exMode, 'data.out.path')

#HDFS API's
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSytem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

fs = FileSytem.get(Configuration())

if(fs.exists(Path(dataInPath)) == True):
    if(fs.exists(Path(dataOutPath))):
        dataOutPath = dataOutPath+"_"+str(datetime.datetime.now().strftime("%y_%m_%d_%H_%M"))
        print("New Path = ",dataOutPath)

    textRDD = sc.textFile(dataInPath)
    wordsRDD = textRDD.flatMap(lambda line: line.split(" "))
    wordCoutRDD = wordsRDD.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    wordCoutRDD.saveAsTextFile(dataOutPath)
else:
    print("Input Path is not exist"+dataInPath)