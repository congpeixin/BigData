package Moudle.Spark1.sparkSQL

import org.apache.spark.sql.{SQLContext, Row, SparkSession}
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



    //=======利用createDF方法创建 createDataFrame(RDD[Person])
    val studentDF1 = spark.sqlContext.createDataFrame(studentRDD)
    studentDF1.show()




    //    studentDF.registerTempTable("students")
    //    val teenagerDF = sqlContext.sql("select * from students where age <= 18")
    //    val teenagerRDD = teenagerDF.rdd
    //
    //    // 这个scala版本比java的版本呢要亲和一点,没有像JAVA一样按照字典排序,而是保证了我们的这个顺序！
    //    teenagerDF.map(row => Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt))
    //      .collect().foreach(stu => println(stu.id + ":" + stu.name + ":" + stu.age))
    //
    //    // 首先scala中保证了顺序的一致,见上面,其次scala中对这个row的使用,比java的row使用更丰富
    //    teenagerRDD.map(row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))
    //      .collect().foreach(stu => println(stu.id + ":" + stu.name + ":" + stu.age))
    //
    //    teenagerRDD.map(row => {
    //      val map = row.getValuesMap[Any](Array("id","name","age"))
    //      Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
    //    }).collect().foreach(stu => println(stu.id + ":" + stu.name + ":" + stu.age))
  }
}
