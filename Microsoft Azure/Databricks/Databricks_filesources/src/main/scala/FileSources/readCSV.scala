package FileSources
import org.apache.spark.sql.{DataFrame, SparkSession}

class readCSV {
  def read(path: String): DataFrame = {
      val spark = SparkSession.builder().
        appName("csv").
        master("local[4]").getOrCreate()
    val csvDF = spark.read.format("csv").
      option("delimeter", "|").
      option("header", "true").
      option("inferSchema", "true").
      load(path)

    csvDF
  }
  def write(dataframe: DataFrame, savePath: String): Unit = {
    dataframe.write.format("csv").
      option("delimeter", "|").
      option("header", "true").
      option("inferScehma", "true").save(savePath)
  }
}
