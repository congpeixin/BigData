package Moudle.scala

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

import scala.collection.immutable.List

/**
  * Created by root on 2017/6/18.
  */
object mappartitionTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class User(id: Int, name: String, pwd: String, sex: Int)
  //创建mysql连接对象
  var conn: Connection = null
  var ps: PreparedStatement = null
  val sql = "INSERT INTO mapp_copy (id,name,pwd,sex) VALUES (?,?,?,?)"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val mappRDD = spark.sparkContext.textFile("data/file3.json").repartition(3)

    val mapRDD3 = mappRDD.mapPartitions(it => {
//      val list = ListBuffer[JSONObject]()
      var list: List[JSONObject] = List()
      conn = DriverManager.getConnection("jdbc:mysql://192.168.133.125:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root")
      while (it.hasNext){
        val line = it.next()
        val json = new JSONObject(line)
        ps = conn.prepareStatement(sql)
        ps.setInt(1,json.get("id").asInstanceOf[Int])
        ps.setString(2,json.get("name").toString)
        ps.setString(3,json.get("pwd").toString)
        ps.setInt(4,json.get("sex").asInstanceOf[Int])
        ps.executeUpdate()
        println("插入成功")
        list.:+(json)
        list.foreach(println(_))
      }
      list.iterator
    }).count() //count()算子起到了Action触发job执行的作用


    spark.stop()
  }
}
