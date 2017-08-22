package Moudle.JDBC

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by cluster on 2017/4/6.
  */
object MySQL {

  var connection: Connection = null
  var statement: PreparedStatement = null

  val sql_create = "CREATE TABLE CHILD (NAME char(20), AGE INT(4))"
  val sql_insert = "INSERT INTO CHILD VALUES (\"cong\",22)"
  val sql_search = "SELECT * FROM CHILD"
  val sql_drop = "DROP TABLE CHILD"

  /**
    * 创建mysql表
    *
    * @param sql
    */
  def createTable(sql: String): Unit ={
    try {
      connection = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/cpeixinTest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
      statement = connection.prepareStatement(sql)
      println(s"---- Begin executing sql: $sql ----")
      val result = statement.executeUpdate() //用于 INSERT、UPDATE 或 DELETE 、CREATE TABLE 和 DROP
      println(s"---- Done executing sql: $sql ----")
    }finally {
      if (null != statement) {
        statement.close()
      }
      if (null != connection) {
        connection.close()
      }
    }
  }

  def insertData(sql: String): Unit ={
    try{
      connection = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/cpeixinTest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
      statement = connection.prepareStatement(sql)
      println(s"---- Begin executing sql: $sql ----")
      val result = statement.executeUpdate() //用于 INSERT、UPDATE 或 DELETE 、CREATE TABLE 和 DROP
      println(s"---- Done executing sql: $sql ----")
    }finally {
      if (null != statement) {
        statement.close()
      }
      if (null != connection) {
        connection.close()
      }
    }
  }

  def dropTable(sql: String): Unit ={
    try{
      connection = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/cpeixinTest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
      statement = connection.prepareStatement(sql)
      println(s"---- Begin executing sql: $sql ----")
      val result = statement.executeUpdate() //用于 INSERT、UPDATE 或 DELETE 、CREATE TABLE 和 DROP
      println(s"---- Done executing sql: $sql ----")
    }finally {
      if (null != statement) {
        statement.close()
      }
      if (null != connection) {
        connection.close()
      }
    }
  }

  def searchData(sql: String): Unit ={
    try{
      connection = DriverManager.getConnection("jdbc:mysql://192.168.31.7:3306/cpeixinTest?useUnicode=true&characterEncoding=UTF-8", "root", "dp12345678")
      statement = connection.prepareStatement(sql)
      println(s"---- Begin executing sql: $sql ----")
      val result = statement.executeQuery()
      val resultMetaData = result.getMetaData
      val colNum = resultMetaData.getColumnCount
      for (i <- 1 to colNum) {
        print(resultMetaData.getColumnLabel(i) + "\t")
      }
      println()
      while (result.next()) {
        for (i <- 1 to colNum) {
          print(result.getString(i) + "\t")
        }
        println()
      }
      println(s"---- Done executing sql: $sql ----")
    }finally {
      if (null != statement) {
        statement.close()
      }
      if (null != connection) {
        connection.close()
      }
    }

  }


  def main(args: Array[String]) {


    createTable(sql_create)
    insertData(sql_insert)
    searchData(sql_search)
  }

}
