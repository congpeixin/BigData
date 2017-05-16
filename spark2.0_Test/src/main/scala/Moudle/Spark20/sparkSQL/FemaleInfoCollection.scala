package Moudle.Spark20.sparkSQL

import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/4/6.
  */
object FemaleInfoCollection {
  case class FemaleInfo(name: String, gender: String, stayTime: Int)
  def main(args: Array[String]) {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[4]")  //本地运行
      .appName("sparkCore")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val text = spark.sparkContext.textFile("C:\\Users\\DN\\IdeaProjects\\spark2.0\\spark2.0_Test\\src\\main\\scala\\com\\spark2\\module\\spark\\log.txt")
    import spark.implicits._

    /**
      * sqll语句的方式
      */
    val line = text.map(_.split(",")).filter(_.contains("female")).map(w=>{FemaleInfo(w(0),w(1),w(2).toInt)}).toDF().createTempView("netTime")
    val result = spark.sql("select name,sum(stayTime) as stayTime from netTime group by name").filter("stayTime >= 120")

    result.foreach(println(_))







  }
}
