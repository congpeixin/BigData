package Moudle.HBase

/**
  * Created by cluster on 2017/4/6.
  */

import java.io.IOException
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable


import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.lucene.queryparser.xml.builders.ConstantScoreQueryBuilder
import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/4/6.
  */
object HBaseClientScala {

  val hBaseConfig = new HBaseConfig
  val hbconf: Configuration = HBaseConfiguration.create()
  var connection: Connection = null
  var table: Table = null
  //hbconf.set(TableInputFormat.INPUT_TABLE,Constants.NAMESPACE+":"+Constants.TABLENAME)
  hbconf.set("hbase.zookeeper.quorum",hBaseConfig.getQuorum)
  hbconf.set("hbase.zookeeper.property.clientPort", hBaseConfig.getPort)
  hbconf.set("zookeeper.znode.parent", "/hbase-unsecure")
  hbconf.set("hbase.client.retries.number", "3")




  /**
    * 创建单列簇的表
    *
    * @param namespace 命名空间
    * @param tbName 表名
    * @param clFamily 列簇
    */
  def createTable(namespace: String ,tbName: String , clFamily: String): Unit ={
    //创建和HBase的连接
    connection = ConnectionFactory.createConnection(hbconf)
    val admin = connection.getAdmin
    var isNameSpaceExist: Boolean = false
    try {
      val nsd: NamespaceDescriptor = admin.getNamespaceDescriptor(namespace)
      isNameSpaceExist = true
    } catch {
      case e :Exception =>{ println("Exception")}
    }
    if (!isNameSpaceExist){
      admin.createNamespace(NamespaceDescriptor.create(namespace).build())
      println(namespace+"create success!")
    }
    val tableNameWithNameSpace: String  = namespace + ":" + tbName
    val tableName = TableName.valueOf(tableNameWithNameSpace)
    val tableDescr = new HTableDescriptor(tableName)
    //如果是创建多列簇的表，可以在这里添加循环
    tableDescr.addFamily(new HColumnDescriptor(clFamily.getBytes))
    println("Creating table "+ tableName)
    if (admin.tableExists(tableName)){
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
    admin.createTable(tableDescr)
    connection.close()
    println("done !!!")
  }

  /**
    * 插入数据，传入参数可以通过循环得到
    *
    * @param namespace
    * @param tbName
    * @param columnFamily
    * @param colName
    * @param value
    * @param rowKey
    */
  def putData(namespace: String ,tbName: String, columnFamily: String, colName: String, value: String, rowKey: String): Unit = {
    try {
      //获取hbase连接
      connection = ConnectionFactory.createConnection(hbconf)
      //获取table对象
      table = connection.getTable(TableName.valueOf(namespace + ":" + tbName))
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), Bytes.toBytes(value))
      table.put(put)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      if (table != null) {
        try {
          // 关闭HTable对象
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
      if (connection != null) {
        try {
          //关闭hbase连接.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }

  /**
    * 插入数据，传入参数为 Put 对象
    *
    * @param namespace
    * @param tbName
    * @param put
    */
  def putData(namespace: String ,tbName: String,put: Put): Unit = {
    try {
      //获取hbase连接
      connection = ConnectionFactory.createConnection(hbconf)
      //获取table对象
      table = connection.getTable(TableName.valueOf(namespace + ":" + tbName))
      table.put(put)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      if (table != null) {
        try {
          // 关闭HTable对象
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
      if (connection != null) {
        try {
          //关闭hbase连接.
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }


  def main(args: Array[String]) {
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[4]")  //本地运行
      .appName("sparkCore")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //    createTable(Constants.NAMESPACE,Constants.TABLENAME,"info")
    //    putData("cpx","0406","info","name","cong","row-2017")
    //    putData("cpx","0406","info","name","huhu","row-2016")
    //声明要查的表的信息
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("info"))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbconf.set(TableInputFormat.INPUT_TABLE, "cpx:0406")
    hbconf.set(TableInputFormat.SCAN, scanToString)

    /**
      * 查询HBase中的数据
      */
    //通过spark接口获取表中的数据
    val rdd = spark.sparkContext.newAPIHadoopRDD(hbconf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
    //遍历hbase表中的每一行，并打印结果
    rdd.collect().foreach(x => {
      val key = x._1.toString
      val it = x._2.listCells().iterator()
      while (it.hasNext) {
        val c = it.next()
        val family = Bytes.toString(CellUtil.cloneFamily(c))
        val qualifier = Bytes.toString(CellUtil.cloneQualifier(c))
        val value = Bytes.toString(CellUtil.cloneValue(c))
        val tm = c.getTimestamp
        println(" Family=" + family + " Qualifier=" + qualifier + " Value=" + value + "TimeStamp=" + tm)
      }
    })
    spark.stop()
  }
}

