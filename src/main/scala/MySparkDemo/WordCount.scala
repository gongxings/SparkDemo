
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "D:\\济南拥堵数据\\m=06\\d=01\\w=6\\00"
    //本地运行 spark
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)

    /*val wordCount = textFile.flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)
    //wordCount.foreach(println)*/

   /* val data = textFile.map(lines =>({
      val details = lines.split(",")
      details(0)+details(1)+details(2)+details(3)+details(4)+details(5)
    },lines.split(",").toList))

    val dataFilter = textFile.filter(lines=>({
      val values = lines.split(",")
      values(5).equals("2")//&&values(6).equals(2)
    }))*/

    //wordCount.saveAsTextFile("00result.txt")
    //println(dataFilter.collect().toBuffer)
    //把数据分成 key,vlaue的元组
    val data = textFile.map(x=>{
     val line =  x.split(",")
      //key = AreaID + MapVersion+TimeStamp+MeshID+Number+layer
      val key = line(0)+"_"+line(1)+"_"+line(2)+"_"+line(3)+"_"+line(4)+"_"+line(5)
     /* var value = line(6)+","+line(7)+","+line(8)+","+line(9)
      if(line.length>10){
        value +=","+line(10)+","+line(11)+","+line(12);
      }*/
      (key,line.toList)
    })
    //println(data.collect().toBuffer)
    //过滤元组
    val filterData: RDD[(String, List[String])] = data.filter(_._2.size>10)
      .filter(_._2(8).equals("3"))
    filterData.groupBy(_._1)
    filterData.saveAsTextFile("00res.txt")
   //println(filterData.collect().toBuffer)
    sc.stop()
  }
}