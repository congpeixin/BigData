package Moudle.Spark2.core

import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/5/25.
  */
object SparkSessionTest {
  val warehouseLocation = "/spark-warehouse"
  val spark = SparkSession
    .builder().master("local[4]")  //本地运行
    .appName("sparkCore")
    .config("spark.sql.warehouse.dir", warehouseLocation) //用户当前的工作目录
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

}
