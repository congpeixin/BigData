package com.Pool

import com.mongodb.{ServerAddress, MongoClient}
import org.json4s.jackson.Json


/**
  * Created by cluster on 2017/5/5.
  */
object MongoPool {

  var  instances = Map[String, MongoClient]()

  //node1:port1,node2:port2 -> node
  def nodes2ServerList(nodes : String):java.util.List[ServerAddress] = {
    val serverList = new java.util.ArrayList[ServerAddress]()
    nodes.split(",")
      .map(portNode => portNode.split(":"))
      .flatMap{ar =>{
        if (ar.length==2){
          Some(ar(0),ar(1).toInt)
        }else{
          None
        }
      }}
      .foreach{case (node,port) => serverList.add(new ServerAddress(node, port))}

    serverList
  }

  def apply(nodes : String) : MongoClient = {
    instances.getOrElse(nodes,{
      val servers = nodes2ServerList(nodes)
      val client =  new MongoClient(servers)
      instances += nodes -> client
      println("new client added")
      client
    })
  }

  def main(args: Array[String]) {
    val nodes = "192.168.31.7:27017"
    val client = MongoPool(nodes)
    val coll2 = client.getDatabase("test").getCollection("sku_0317_test")

    val aa = coll2.find()

  }
}
