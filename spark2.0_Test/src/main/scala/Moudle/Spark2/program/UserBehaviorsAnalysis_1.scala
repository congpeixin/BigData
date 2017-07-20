package Moudle.Spark2.program

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And

/**
  * Created by cluster on 2017/7/19.
  * http://blog.csdn.net/duan_zhihua/article/details/53025008
  */

case class UserLog(logID:String,userID:String,time:String,typed:String,location:String,consumed:Double)
case class UserInfo(userID:Long,name:String,Gender:String,Occupation:String)
case class LogOnce(logID:Long,userID:Long,count:Long )
case class ConsumOnce(logID: String, userID: String, consumed: Double)

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

    //读取json文件
    val userLogjson = spark.read.json("data/userLog.json")
    val userInfojson = spark.read.json("data/userInfo.json")

    //生成parquet文件
//    userLogjson.write.format("parquet").save("data/userLog.parquet")
//    userInfojson.write.format("parquet").save("data/userInfo.parquet")

    //读取parquet文件
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
    //      .show()
    //    +----------+--------+------------+
    //    |    userID|    name|userlogCount|
    //    +----------+--------+------------+
    //    |userID3234|  wangwu|           2|
    //    |userID1234|zhangsan|           1|
    //    +----------+--------+------------+

    //购买金额前3的用户  consumed
    userLog.join(userInfo,userInfo("userID") === userLog("userID"))
      .groupBy(userInfo("userID"),userInfo("name"))
      .agg(round(sum(userLog("consumed")),2).alias("money"))
      .sort($"money".desc)
      .limit(3)
    //      .show()

    //转化成DS,利用sql语句查询
    val userLogDS = userLog.as[UserLog]
    userLogDS.createTempView("userlog")
    spark.sql("select * from userlog").show()

    ///或者注册之后前10天内购买商品总额排名前5为的人
    userLog.join(userInfo, userLog("userID") === userInfo("userID"))
      .filter(userInfo("registeredTime") >= "2016-11-1"
        && userInfo("registeredTime") <= "2016-11-20"
        && userLog("time") >= userInfo("registeredTime")
        && userLog("time") <= date_add(userInfo("registeredTime"), 10))
      .groupBy(userInfo("userID"), userInfo("name"))
      .agg(sum(userLog("logID")).alias("logTimes"))
      .sort($"logTimes".desc)
      .limit(5)
      .show()
  }
}
