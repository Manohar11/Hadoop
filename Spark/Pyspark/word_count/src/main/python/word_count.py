from pyspark import SparkConf, SparkContext
import ConfigParser as config
import sys

props = config.RawConfigParser()
props.read("src/main/configs/app.properties")
exMode = sys.argv[1]
conf = SparkConf().\
    setMaster(props.get(exMode, 'deployMode')).\
    setAppName("Word Count")
sc = SparkContext(conf= conf)
textRDD = sc.textFile(props.get(exMode, 'data.in.path'))
wordsRDD = textRDD.flatMap(lambda line: line.split(" "))
wordCoutRDD = wordsRDD.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
wordCoutRDD.saveAsTextFile(props.get(exMode, 'data.out.path'))