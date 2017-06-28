package Moudle.Spark1.sxt.scala

/*
Actor之间相互收发消息
 */
case class Message(content:String, sender:Actor)

class YasakaActor extends Actor{
  def act{
    while (true){
      receive{
        case Message(content,sender) => {println("yasaka received: " + content);  
                                              sender ! 10000000}
      }
    }
  }
}

class GagaActor(val yasakaActor: YasakaActor) extends Actor{
  def act{
    yasakaActor ! Message("Hello, yasaka, I'm gaga. Are you free now?", this)

    var flag = true
    while (flag){
      receive{
        case response:String => println("gaga received: "+ response)
        case response:Int => println("you love xuruyun. you don't love me anymore !"); flag=false
      }
    }
  }
}

object Message {
  def main(args: Array[String]) {
    val yasakaActor = new YasakaActor
    val gagaActor = new GagaActor(yasakaActor)
    yasakaActor.start()
    gagaActor.start()
    
  }
}
