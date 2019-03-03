import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

/**
  * spark sql测试类
  */
object SparkSqlTest {

/*  case class Rtic(
                   areaId: Int
                   , mapVersion: Int
                   , timeStamp: Long
                   , meshId: Int
                   , Number: Int //路链号
                   , layer: Int //道路分层
                   , rticKind: Int //道路等级
                   , rticTravelTime: Int //旅行时间
                   , los: Int //拥堵程度
                   , sectionCount: Int //拥堵路段数
                   , sectionLos: Int //路段拥堵程度
                   , distance: Int //与路链终点的距离（m）
                   , affectLength: Int //拥堵长度（m）
                 )*/

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local")
      .appName("SparkSqlTest")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val textFile: RDD[String] = sc.textFile("D:\\济南拥堵数据\\m=06\\d=01\\w=6\\00")

    val schema = StructType(
      Seq(
        StructField("areaId", IntegerType, false),
        StructField("mapVersion", IntegerType, false),
        StructField("timeStamp", LongType, false),
        StructField("meshId", IntegerType, false),
        StructField("number", IntegerType, false),
        StructField("layer", IntegerType, false),
        StructField("rticKind", IntegerType, false),
        StructField("rticTravelTime", IntegerType, false),
        StructField("los", IntegerType, false),
        StructField("sectionCount", IntegerType, false),
        StructField("sectionLos", IntegerType, true),
        StructField("distance", IntegerType, true),
        StructField("affectLength", IntegerType, true)
      ))

    val data = textFile.map(x=>{
      val line =  x.split(",")
      //key = AreaID + MapVersion+TimeStamp+MeshID+Number+layer
      val key = line(0)+"_"+line(1)+"_"+line(2)+"_"+line(3)+"_"+line(4)+"_"+line(5)
      /* var value = line(6)+","+line(7)+","+line(8)+","+line(9)
       if(line.length>10){
         value +=","+line(10)+","+line(11)+","+line(12);
       }*/
      (line.toList)
    })
    //println(data.collect().toBuffer)
    //过滤元组
    val filterData: RDD[(List[String])] = data.filter(_.size>10).filter(_(8).equals("3")).cache()

    val rowRDD = filterData
        //.map(_+"0,0,0".split(","))
      .map(attributes => Row(attributes(0).toInt, attributes(1).toInt, attributes(2).toLong, attributes(3).toInt,
        attributes(4).toInt, attributes(4).toInt, attributes(6).toInt, attributes(7).toInt,
        attributes(8).toInt, attributes(9).toInt, attributes(10).toInt, attributes(11).toInt,
        attributes(12).toInt))

    // Apply the schema to the RDD
    val rticDF = spark.createDataFrame(rowRDD, schema)

    /*val rticDF = data.map(_.split(","))
      .map(attributes => Rtic(attributes(0).toInt, attributes(1).toInt, attributes(2).toLong, attributes(3).toInt,
                              attributes(4).toInt, attributes(4).toInt, attributes(6).toInt, attributes(7).toInt,
                              attributes(8).toInt, attributes(9).toInt, attributes(10).toInt, attributes(11).toInt,
                              attributes(12).toInt))
      .toDF*/
    //rticDF.na.fill(0)
    rticDF.createOrReplaceTempView("rtic")

    //rticDF.printSchema();
    //rticDF.limit(100).show(10)
    spark.sql("select * from rtic where sectionCount>=3 ").show()


   spark.stop()

  }

}