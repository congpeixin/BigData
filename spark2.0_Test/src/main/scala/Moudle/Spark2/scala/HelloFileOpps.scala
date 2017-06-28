package Moudle.Spark2.scala

/**
  * Created by cluster on 2017/3/14.
  */
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.io.Source

@SerialVersionUID(99L)   class DTSpark(val name:String) extends Serializable


object HelloFileOpps extends App{
  val dtspark = new DTSpark("spark")

  /** Serialize an object using Java serialization */
  def serialize[T](o: T)  = {
    //  val bos = new ByteArrayOutputStream()
    val bos = new FileOutputStream("D:\\spark.txt")
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    //  bos.toByteArray
  }
  //   println(serialize(dtspark))
  // println(new String(serialize(dtspark)))
  serialize(dtspark)
  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    // val bis = new ByteArrayInputStream(bytes)
    val bis = new FileInputStream("D:\\spark.txt")
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  // println(deserialize[DTSpark](serialize[DTSpark](dtspark)).name)
  println(deserialize[DTSpark]( null).name)

  for (line <- Source.fromFile("D:\\spark.txt","GBK").getLines()){
    println(line)
  }

  println(Source.fromFile("D:\\spark.txt","GBK").mkString)

  for (item <- Source.fromFile("D:\\spark.txt","GBK") ) println(item)
  println("================")
//  println(Source.fromURL("http://spark.apache.org/","UTF-8").mkString)




  //////////////////////


  /** Serialize an object using Java serialization */
  /* def serialize[T](o: T): Array[Byte] = {
     val bos = new ByteArrayOutputStream()
     val oos = new ObjectOutputStream(bos)
     oos.writeObject(o)
     oos.close()
     bos.toByteArray
   }
   //   println(serialize(dtspark))
    println(new String(serialize(dtspark)))

   *//** Deserialize an object using Java serialization *//*
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

   println(deserialize[DTSpark](serialize[DTSpark](dtspark)).name)
*/





}

