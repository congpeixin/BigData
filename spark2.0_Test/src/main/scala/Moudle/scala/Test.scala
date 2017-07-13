package Moudle.scala

import org.apache.spark.sql.SparkSession


/**
  * Created by cluster on 2017/2/20.
  */
object Test {

  //获取变量的数据类型
  def getType(o: AnyRef): String = {
    o.getClass.toString
  }

  def main(args: Array[String]) {
    //spark 2.0的配置

    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[2]")
      .appName("test DataSet")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.default.parallelism", "100")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 6)
    spark.conf.set("spark.executor.memory", "2g")

    //创建DataSet
    val ds = spark.range(5,100,5)
    //创建DataFrame

    val langPercentDF = spark.createDataFrame(List(("cong",22),("hu",23),("zhang",33),("zhu",44)))
    langPercentDF.withColumnRenamed("_1","name").withColumnRenamed("_2","age")

    langPercentDF.orderBy("age").show()
  }
}