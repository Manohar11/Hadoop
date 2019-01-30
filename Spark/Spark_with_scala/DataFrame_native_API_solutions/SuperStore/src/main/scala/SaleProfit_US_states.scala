import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs._

object SaleProfit_US_states {
  def main(args: Array[String]): Unit = {
    val props =ConfigFactory.load()
    val exMode = args(0)

    val spark = SparkSession.
      builder().
      master(props.getConfig(exMode).getString("deployMode")).
      appName("Sale Profit Per State in US").
      getOrCreate()

    //Handling with file system
    val dataInPath = props.getConfig(exMode).getString("data.in.path")
    var dataOutPath = props.getConfig(exMode).getString("data.out.path")

    //HDFS API's

    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    if(fs.exists(new Path(dataInPath)) == true){
      if(fs.exists(new Path(dataOutPath))){
        val hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
        val day = Calendar.getInstance().get(Calendar.DAY_OF_MONTH)
        val month = Calendar.getInstance().get(Calendar.MONTH)+1
        val year = Calendar.getInstance().get(Calendar.YEAR)
        dataOutPath = dataOutPath+"_"+year+"_"+month+"_"+day+"_"+hour
      }
      val dataDF = spark.read.format("csv").
        option("sep", ",").
        option("inferSchema", true).
        option("header", true).
        load(dataInPath)

      //  Type casting - since the inferScheame option doesn't properly infer the data type
      //  df.withColumn(<old column name>,df.col(<dummy column>).cast(<datatype>))
      val dataDF1 = dataDF.
        withColumn("Sales", dataDF.col("Sales").cast(DoubleType)).
        withColumn("Quantity", dataDF.col("Quantity").cast(IntegerType)).
        withColumn("Discount", dataDF.col("Discount").cast(DoubleType))

      val dataAggDF = dataDF1.
        select(col("Country"), col("State"), col("Profit")).
        groupBy(col("Country"), col("State")).
        agg(round(sum((col("Profit"))), 2).alias("Total_profit"))

      dataAggDF.orderBy(col("Total_profit").desc).
        coalesce(2).
        write.csv(dataOutPath)
    }
    else {
      println("Input Path is not exist" + dataInPath)
    }
  }

}
