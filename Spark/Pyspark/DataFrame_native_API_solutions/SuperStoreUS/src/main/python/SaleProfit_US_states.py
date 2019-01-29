import sys
import configparser as config
import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DoubleType
    from pyspark.sql.types import IntegerType
    from pyspark.sql.functions import *

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

        dataDF = spark.read.csv(dataInPath,
                            header= True, inferSchema= True)

        #Type casting - since the inferScheame option doesn't properly infer the data type
        dataDF = dataDF.withColumn("Sales", dataDF.Sales.cast(DoubleType())).\
            withColumn("Quantity", dataDF.Quantity.cast(IntegerType())).\
            withColumn("Discount", dataDF.Discount.cast(DoubleType()))

        dataAggDF = dataDF.select(dataDF.Country, dataDF.State, dataDF.Profit).\
            groupBy("Country", "State").\
            agg(round(sum(dataDF.Profit), 2).alias("Total_profit"))

        dataAggDF.orderBy(dataAggDF.Total_profit.desc()).\
            coalesce(2).\
            write.\
            csv(dataOutPath, mode= "overwrite")
    else:
        print("Input Path is not exist" + dataInPath)

except ImportError as ie:
    print("Import Error with Spark modules", ie)
    sys.exit(1)