package com.spark2Test.study.scala

/**
  * Created by cluster on 2017/3/14.
  */
class Dataframework
case class Computerframework (name:String,popular:Boolean) extends Dataframework
case class Storgeframework (name:String,popular:Boolean) extends Dataframework

object helloPatternMatch {
  def main(args: Array[String]): Unit = {
    // getSalary("hadoop")
    // getSalary("flink")
    // getSalary("scala")

    getSalary("scalasafdas",6)
    getMatchType(100.0)
    getMatchType("java")
    getValue("spark",Map("spark"->"the hosttest!!"))

    getBigDataType(Computerframework("spark",true))

    getMatchTypeCollection(Array("scala","java"))

  }

  def getSalary(name:String,age:Int){
    name match{
      case "spark" => println("$ 15k")
      case "hadoop" =>  println("$ 12k")
      case _ if name == "scala" =>  println("$ 14k")
      case _ if name == "flink" =>  println("$ 13k")
      case _name if age >= 5 =>  println(" name : "+ _name +"  $ 16k")

      case _ =>  println("$ 8k")
    }
  }

  def getMatchType(msg:Any){
    msg match {
      case i: Int => println("integer")
      case s:String => println("String")
      case s:Double => println("Double")
      case array:Array[Int]=>  println("Array")
      case _ => println("other type")
    }
  }

  def getMatchTypeCollection(msg:Array[String]){
    msg match {
      case   Array("scala") => println("1   element")
      case  Array("scala","java")=> println("2  element")
      case  Array("spark",_*) => println("many element")
      case _ => println("other element")
    }
  }

  def getBigDataType(data:Dataframework){
    data match {
      case Computerframework(name,popular) =>  println("name:  "+name+" "+ popular)
      case Storgeframework(name,popular) =>   println(name+" "+ popular)
      case _ =>   println("other types")
    }



  }

  def getValue(key:String,content: Map[String,String]){
    content.get(key) match {
      case Some(value) => println(value)
      case None => println("none")
    }
  }


}