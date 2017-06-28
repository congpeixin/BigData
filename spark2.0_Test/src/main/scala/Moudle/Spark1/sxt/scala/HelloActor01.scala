package Moudle.Spark1.sxt.scala

/*
给Actor发消息
 */
class HelloActor extends Actor{
  def act(){
    while (true){
      receive{
        case name:String => println("Hello, " + name)
        case money:Int => println("How much? " + money)
        case _ => "other"
      }
    }
  }
}

object HelloActor{
  def main(args: Array[String]) {
    val helloActor = new HelloActor
    helloActor.start()
    helloActor ! 100
  }
}