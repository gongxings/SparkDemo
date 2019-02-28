import org.apache.spark.{SparkConf, SparkContext}

object ScalaTest{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd01 = sc.makeRDD(List(370100,1501,1527782520,555606,197,2,5,9,1,0))
    val r01 = rdd01.map{ x =>x+1}
    println(r01.collect().mkString("_"))
  }
}
