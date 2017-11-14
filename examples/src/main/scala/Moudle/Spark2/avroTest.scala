package Moudle.Spark2

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
/**
  * Created by root on 2017/10/30.
  */
case class info(year: Int,month: Int,title: String,rating: Float)
object avroTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.createDataset(Seq(9,8,"cong",0)).toDF("year","month","name","num")

    df.toDF.write.partitionBy("year", "month").avro("data/avro")
  }
}
