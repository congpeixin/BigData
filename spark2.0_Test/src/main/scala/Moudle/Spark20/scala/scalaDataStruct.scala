package Moudle.Spark20.scala

/**
  * Created by cluster on 2017/3/13.
  */
object scalaDataStruct {
  def main(args: Array[String]): Unit = {
    //def apply[A, B](elems: (A, B)*): Map[A, B]
//    val persons =scala.collection.immutable.SortedMap(("2scala",1),("1dtspark",2))
//    for ((name,age) <-persons) println (name + "  "+age)

//    val bigDatas = Map(" spark" -> 6,"Hadoop"->11 )
//    // value update is not a member of scala.collection.immutable.Map[String,Int]
////     bigDatas("spark") =10
//    for ((classname,grade)<- bigDatas) println(classname +"    "+grade)
//
//    val programingLanguage = scala.collection.mutable.Map("SCALA"->2,"JAVA"->4,"python"->"")
//    programingLanguage("SCALA") =15
//    for ((name,age) <-programingLanguage) println (name + "  "+age)
////    for ((name,age) <-programingLanguage) println (name + "  "+age)
//
//
////
////    // println (programingLanguage("Python")) //java.util.NoSuchElementException: key not found: python
////
//    println (programingLanguage.getOrElse( "python",5)) //getOrElse提供了默认值

//    for ((name,age) <-programingLanguage) println (name + "  "+age)
//
//     val personsInfomations= scala.collection.mutable.HashMap[String,Int]()
    val personsInfomations= scala.collection.mutable.LinkedHashMap[String,Int]()
////
    personsInfomations += (" sparkaaa" -> 6,"hadoopaaa"->100,"python"->700 )
////
//    personsInfomations -= (" sparkaaa"  )
//    for ((name,age) <-personsInfomations) println (name + "  "+age)
//
//    for(key<-personsInfomations.keySet) println(key)
//    for(value<-personsInfomations.values) println(value)
//
    val result= for ((name,age) <-personsInfomations) yield (age,name)


    for ((name,age) <-result) println (name + " : "+age)

//    val information = ("spark  ","  male",30,"I love spark")
//    println(information._1 +information._2)

  }
}
