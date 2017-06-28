//package com.cong.scala
//
//import java.sql.DriverManager
//
//object ScalaMysql {
//
//  def main(args: Array[String]) {
//    // create database connection
//    val dbc = "jdbc:mysql://spark003:3306/test?user=root&password=123123"
//    classOf[com.mysql.jdbc.Driver]
//    val conn = DriverManager.getConnection(dbc)
//
//    // do database insert
//    try {
//      val prep = conn.prepareStatement("INSERT INTO blog (title, body) VALUES (?, ?) ")
//      prep.setString(1, "Nothing great was ever achieved without enthusiasm.")
//      prep.setString(2, "Ralph Waldo Emerson")
//      prep.executeUpdate
//    }
//    finally {
//      conn.close
//    }
//  }
//}