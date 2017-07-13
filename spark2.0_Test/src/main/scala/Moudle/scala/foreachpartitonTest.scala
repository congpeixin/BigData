package Moudle.scala

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

/**
  * Created by root on 2017/6/21.
  */
object foreachpartitonTest {
  def toMySQL(iterator: Iterator[String]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO mapp (id,name,pwd,sex) VALUES (?,?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://192.168.133.125:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root")
      iterator.foreach(str => {
        val json = new JSONObject(str)
        ps = conn.prepareStatement(sql)
        ps.setInt(1,json.get("id").asInstanceOf[Int])
        ps.setString(2,json.get("name").toString)
        ps.setString(3,json.get("pwd").toString)
        ps.setInt(4,json.get("sex").asInstanceOf[Int])
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sparkToMysql").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataFromHDFS=sc.textFile("data/file3.json").repartition(3).map(line => line.toString)
    dataFromHDFS.foreachPartition(toMySQL)


  }
}
