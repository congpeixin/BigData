package Moudle.Spark2.program

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And

/**
  * Created by cluster on 2017/7/19.
  * http://blog.csdn.net/duan_zhihua/article/details/53025008
  */
//{"logID":"logID5555", "userID":"userID5234","time":"20161103","typed":"0","location":"tianjing","consumed":"500"}
case class UserLog(logID:Long,userID:Long,time:String,typed:Int,location:String,consumed:Double)
//{"userID":"userID5234","Name":"zhangsan","Gender":"man","Occupation":"student"}
case class UserInfo(logID:Long,userID:Long,time:String,typed:Int,location:String,consumed:Double)
case class LogOnce(logID:Long,userID:Long,count:Long )

object UserBehaviorsAnalysis_1 {
  def main(args: Array[String]) {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession.builder()
      .appName("userBehaviors_1")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    val userLogjson = spark.read.json("data/userLog.json")
    val userInfojson = spark.read.json("data/userInfo.json")

//    userLogjson.write.format("parquet").save("data/userLog.parquet")
//    userInfojson.write.format("parquet").save("data/userInfo.parquet")

    val userInfo = spark.read.format("parquet").parquet("data/userInfo.parquet")
    val userLog = spark.read.format("parquet").parquet("data/userLog.parquet")

    val startTime= "'2016-11-1 00:00:00'"
    val endTime = "'2016-11-2 23:59:59'"

    //统计今天购买次数最多的  Top5 日志条数count
    userLog.filter("time >= "+startTime+"and time <="+endTime)
      .join(userInfo, userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(count(userLog("logID")).alias("userlogCount"))//DataFrame.groupBy().agg()  在分组之后，对分组之后的数据进行聚合操作
      .sort($"userlogCount".desc)
      .limit(5)
      .show()
//    +----------+--------+------------+
//    |    userID|    name|userlogCount|
//    +----------+--------+------------+
//    |userID3234|  wangwu|           2|
//    |userID1234|zhangsan|           1|
//    +----------+--------+------------+

    //购买金额前5的用户





  }
}
