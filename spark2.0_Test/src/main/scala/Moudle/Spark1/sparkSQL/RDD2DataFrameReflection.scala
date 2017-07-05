package Moudle.Spark1.sparkSQL

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


case class Student(id:Int, name:String, age:Int)
/**
  * Created by root on 2016/7/1.
  */
object RDD2DataFrameReflection {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[4]")  //本地运行
      .appName("sparkCore")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // 在Scala中使用反射方式进行RDD到DataFrame的转换,需要手动导入一个隐式转换
    import sqlContext.implicits._


    //======创建RDD[String]======
    val studentStringRDD = sc.textFile("data/student.txt")

    //======利用反射的方式 将 RDD[String] --> RDD[Student]======
    val studentRDD = studentStringRDD.map(line => line.split(","))
      .map(array => Student(array(0).trim.toInt,array(1),array(2).trim.toInt))
    //    获取RDD[Student]中的对象或者属性
    //    studentRDD.foreach(println(_))//打印对象
    //    studentRDD.foreach(student =>println(student.id,student.name,student.age))//打印属性

    //====== RDD[Student] --> RDD[Row]======
    val studentRowRDD = studentRDD.map(student => Row(student.id,student.name,student.age))
    //    studentRowRDD.foreach(println(_))
    //打印属性  注意，Row类型数据，获取属性是通过Row下标来获取的，下标值从0开始
    //    studentRowRDD.foreach(student =>println(student.get(0),student.get(1),student.get(2)))

    //====== RDD[Row] --> JavaRDD[Row]======
    val studentJavaRowRDD = studentRowRDD.toJavaRDD()


    //=====RDD 转换 DF : 此种转换是 利用 case class的RDD[Student] --（直接 .toDF()）--> DF
    val studentDF = studentRDD.toDF() //本机环境是spark 2.0，用的api都是spark2.0的，所以要创建sparksession的代码。
    //打印里面的数据
    //    studentDF.show()
    //    studentDF.foreach(student => println(student.getAs[Long]("id"),student.getAs[String]("name"),student.getAs[Long]("age")))

    //=====RDD 转换 DF :  createDataFrame(RDD[Person])
    val studentDF1 = spark.sqlContext.createDataFrame(studentRDD)
//    studentDF1.show()

    //=====RDD 转换 DF :  利用schema structure创建DataFrame,createDataFrame(RDD[Row])
    val schema = StructType(
      Seq(StructField("id",IntegerType,true),StructField("name",StringType,true),StructField("age",IntegerType,true)
      )
    )
    val studentDF2 = spark.createDataFrame(studentRowRDD,schema)
//    studentDF2.show()

    studentDF.registerTempTable("students")
    val ageDF = sqlContext.sql("select * from students where age > 18")
    ageDF.foreach(person => println(person.getAs[String]("name")))

  }
}
