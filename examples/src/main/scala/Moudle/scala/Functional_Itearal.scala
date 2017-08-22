package Moudle.scala

/**
  * Created by cluster on 2017/3/14.
  */
object Functional_Itearal {
  def main(args: Array[String]): Unit = {

    val range = 1 to 10
    val list = List(1,2,3,4,5)
    println(range)
    println(list.head)
    println(list.tail)


    println(list.tail)

    println(0::list)
    var linkedList = scala.collection.mutable.LinkedList(1,2,3,4,5)
    println(linkedList.elem)
    println(linkedList.tail)

    while(linkedList != Nil){
      println(linkedList.elem)
      linkedList = linkedList.tail
    }

    println(linkedList)

  }
}
