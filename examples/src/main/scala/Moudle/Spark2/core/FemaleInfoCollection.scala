package Moudle.Spark2.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 计算步骤：过滤性别-->按逗号分隔-->组织数据格式-->同的key,value相加-->过滤value大于120的-->按照value值进行排序
  * Created by cluster on 2017/4/5.
  */
object FemaleInfoCollection {
  Logger.getLogger("debug").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[4]")  //本地运行
      .appName("sparkCore")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val text = spark.sparkContext.textFile("C:\\Users\\DN\\IdeaProjects\\spark2.0\\spark2.0_Test\\src\\main\\scala\\com\\spark2\\module\\spark\\log.txt")

    /**
      * LiuYang,female,20
      * YuanJing,male,10    数据格式
      */
    val line = text.filter(_.contains("female")).map(w=>{val word =  w.split(",")
      (word(0),word(2).toInt)
    }).reduceByKey(_+_).filter(w => w._2 > 120).sortBy(_._2,false)

    line.foreach(println)
  }
}
