package Moudle.Spark20.DataSet

/**
  * Created by cluster on 2017/3/15.
  */
import org.apache.spark.sql.SparkSession

object DataSetsops_1 {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DatasetOps")
      .master("local")
      .config("spark.sql.warehouse.dir", "/spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    val personDF= spark.read.json("D:\\people.json")
    val personScoresDF= spark.read.json("D:\\peopleScores.json")
    val personDS = personDF.as[Person]

    /*    personDS.map{person=>
          (person.name,if (person.age == null) 0 else person.age +100 )

         }.show()

         personDS.mapPartitions{persons =>
           val result = ArrayBuffer[(String,Long)]()
           while(persons.hasNext){
             val person = persons.next()
             result +=((person.name,person.age+10000))
           }
            result.iterator
         }.show


         personDS.dropDuplicates("name").show
         personDS.distinct().show*/

    println(personDS.rdd.partitions.size)
    println("*************************************1.personDS.rdd.partitions.size*****************************************************************")
    val repartitionDs= personDS.repartition(4)
    println(repartitionDs.rdd.partitions.size)
    println("*************************************2.repartitionDs.rdd.partitions.size*****************************************************************")
    val coalesced= repartitionDs.coalesce(2)
    println(coalesced.rdd.partitions.size)
    println("*************************************3.coalesced.rdd.partitions.size*****************************************************************")
    coalesced.show
    println("*************************************4.coalesced.show*****************************************************************")


    // personDF.show()
    // personDF.collect().foreach (println)
    // println(personDF.count())

    //val personDS = personDF.as[Person]
    // personDS.show()
    // personDS.printSchema()
    //val dataframe=personDS.toDF()

    /* personDF.createOrReplaceTempView("persons")
     spark.sql("select * from persons where age > 20").show()
       spark.sql("select * from persons where age > 20").explain()
     */
    //  val personScoresDF= spark.read.json("G:\\IMFBigDataSpark2016\\spark-2.0.0-bin-hadoop2.6\\examples\\src\\main\\resources\\peopleScores.json")
    // personDF.join(personScoresDF,$"name"===$"n").show()
    /*  personDF.filter("age > 20").join(personScoresDF,$"name"===$"n").show()

     personDF.filter("age > 20")
      .join(personScoresDF,$"name"===$"n")
      .groupBy(personDF("name"))
      .agg(avg(personScoresDF("score")),avg(personDF("age")))
      .explain()
      //.show()

      */

    while(true) {}

    spark.stop()
  }
}
