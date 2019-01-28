import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs._
import java.util.Calendar


object word_count {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val exMode = args(0)

    val conf = new SparkConf().
      setMaster(props.getConfig(exMode).getString("deployMode")).
      setAppName("Word Count")
    val sc = new SparkContext(conf)

    //Handling with file system
    val dataInPath = props.getConfig(exMode).getString("data.in.path")
    var dataOutPath = props.getConfig(exMode).getString("data.out.path")

    //HDFS API's
    val fs = FileSystem.get(sc.hadoopConfiguration)

    if(fs.exists(new Path(dataInPath)) == true){
      if(fs.exists(new Path(dataOutPath))){
        val hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
        val day = Calendar.getInstance().get(Calendar.DAY_OF_MONTH)
        val month = Calendar.getInstance().get(Calendar.MONTH)+1
        val year = Calendar.getInstance().get(Calendar.YEAR)
        dataOutPath = dataOutPath+"_"+year+"_"+month+"_"+day+"_"+hour
      }
      val textRDD = sc.textFile(dataInPath)
      val wordsRDD = textRDD.flatMap(line => line.split(" "))
      val wordCountRDD = wordsRDD.
        map(word => (word, 1)).
        reduceByKey( (v1: Int, v2: Int) => v1 + v2)
      wordCountRDD.saveAsTextFile(dataOutPath)
    }
    else {
      println("Input Path is not exist" + dataInPath)
    }
  }

}
