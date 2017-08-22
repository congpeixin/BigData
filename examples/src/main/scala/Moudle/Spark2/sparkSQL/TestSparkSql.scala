package Moudle.Spark2.sparkSQL

//import util.HDFSHelper
import java.io.{File, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by zls on 16-11-24.
  * http://blog.csdn.net/ronaldo4511/article/details/53379526
  */
object TestSparkSql {

  def start = {
//    prepareDate
    testSql
  }

  val localfile = "/home/zls/Desktop/spark_sql/testsql.txt"
  val hdfsFile = "/test/testsql.txt"

//  def prepareDate = {
//    val dirSpark : File = HDFSHelper.createLocalFile(localfile)
//    val writer : PrintWriter = new PrintWriter(dirSpark)
//    val rand : Random = new Random(System.currentTimeMillis)
//    for (i <- 0 until 100){
//      writer.write("p" + i + "," + rand.nextInt(100) + "\n")
//    }
//    writer.flush
//    writer.close
//
//    val hdfs = FileSystem.get(new Configuration)
//    HDFSHelper.copyFileFromLocal(hdfs, localfile, hdfsFile)
//  }


  case class People(name : String, age : Int)

  def testSql = {
    val sparkSession : SparkSession = SparkSession.builder.appName("test spark sql").getOrCreate
    rddToDfWithCaseClass(sparkSession)
    //rddToDfWithCaseClass2(sparkSession)
    //rddToDfWithStructType(sparkSession)
  }

  def rddToDfWithCaseClass(sparkSession : SparkSession) : Unit = {
    import sparkSession.implicits._
    val rdd : RDD[People]= sparkSession.sparkContext.textFile(hdfsFile,2).map(line => line.split(",")).map(arr => People(arr(0),arr(1).trim.toInt))
    val peopleRdd = rdd.toDF
    peopleRdd.show  // print first 20 records
    peopleRdd.select($"name",$"age").filter($"age">20).show
  }

  def rddToDfWithCaseClass2(sparkSession : SparkSession) : Unit = {
    import sparkSession.implicits._
    val rdd : RDD[People]= sparkSession.sparkContext.textFile(hdfsFile,2).map(line => line.split(",")).map(arr => People(arr(0),arr(1).trim.toInt))
    val peopleRdd = rdd.toDF
    val tableName = "table_people"
    peopleRdd.createOrReplaceTempView(tableName)
    val df = sparkSession.sql("select name,age from " + tableName + " where age > 20")
    df.rdd.collect.foreach(row => println("name: " + row(0) + ", age: " + row(1)))
  }

  def rddToDfWithStructType(sparkSession : SparkSession) : Unit = {
    //set schema structure
    val schema = StructType(
      Seq(
        StructField("name",StringType,true)
        ,StructField("age",IntegerType,true)
      )
    )
    import sparkSession.implicits._
    val rowRDD = sparkSession.sparkContext.textFile(hdfsFile,2).map( x => x.split(",")).map( x => Row(x(0),x(1).trim().toInt))
    val peopleRdd = sparkSession.createDataFrame(rowRDD,schema)
    peopleRdd.show
    peopleRdd.select($"name",$"age").filter($"age">20).show
  }

}
