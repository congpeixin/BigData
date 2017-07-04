package com.cong.sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

case class Student(id:Int, name:String, age:Int)

/**
  * Created by root on 2016/7/1.
  */
object RDD2DataFrameReflection {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // 在Scala中使用反射方式进行RDD到DataFrame的转换,需要手动导入一个隐式转换
    import sqlContext.implicits._
    //利用反射的方式 将 RDD[String] --> RDD[Student]
    val students = sc.textFile("data/student.txt")
        .map(line => line.split(","))
          .map(array => Student(array(0).trim.toInt,array(1),array(2).trim.toInt))
    //获取RDD中的对象或者属性
    students.foreach(println(_))//打印对象
    students.foreach(student =>println(student.id,student.name,student.age))//打印属性



    // 直接使用RDD的toDF()方法即可转换为DataFrame
    val studentDF = students.toDF()
    // 利用createDF方法创建
    val studentDF1 = sqlContext.createDataFrame()

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
