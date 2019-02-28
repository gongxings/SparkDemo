
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "D:\\济南拥堵数据\\m=06\\d=01\\w=6\\00"
    //本地运行 spark
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)

    val wordCount = textFile.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    //wordCount.foreach(println)
    wordCount.saveAsTextFile("00result.txt")
    sc.stop()
  }
}