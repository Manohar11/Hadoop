import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{Config, ConfigFactory}

object word_count {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val exMode = args(0)
    val conf = new SparkConf().
      setMaster(props.getConfig(exMode).getString("deployMode")).
      setAppName("Word Count")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile(props.getConfig(exMode).getString("data.in.path"))
    val wordsRDD = textRDD.flatMap(line => line.split(" "))
    val wordCountRDD = wordsRDD.
      map(word => (word, 1)).
      reduceByKey( (v1: Int, v2: Int) => v1 + v2)
    wordCountRDD.saveAsTextFile(props.getConfig(exMode).getString("data.out.path"))
  }

}
