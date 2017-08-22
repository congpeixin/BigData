//package Moudle.Spark1.sxt.scala
//
///*
//给Actor发送case class的消息
// */
//case class Register(username: String, password:String)
//case class Login(username: String , password:String)
//class UserManagerActor extends Actor{
//  def act{
//    while (true){
//      receive{
//        case Login(username, password) => println("login: " + username + ": " + password)
//        case Register(username, password) => println("register: " + username + ": " + password)
//      }
//    }
//  }
//}
//object UserManagerActor{
//  def main(args: Array[String]) {
//    val userActor = new UserManagerActor
//    userActor.start()
//    userActor ! Register("yasaka", "123")
//    userActor ! Login("yasaka", "123")
//  }
//}