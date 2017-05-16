package Moudle.HBase

import java.io.IOException
import java.util

import org.apache.hadoop.hbase.{CellUtil, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.sql.SparkSession

/**
  * Created by cluster on 2017/4/7.
  */
object SparkHbasetoHbase {
  case class FemaleInfo(name: String, gender: String, stayTime: Int)
  def main(args: Array[String]) {
    if (args.length < 1) {
      printUsage
    }
    val warehouseLocation = "/spark-warehouse"
    val spark = SparkSession
      .builder().master("local[4]")  //本地运行
      .appName("sparkCore")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.spark2.module.spark.HBase.MyRegistrator")
      .getOrCreate()

    // 建立连接hbase的配置参数，此时需要保证hbase-site.xml在classpath中
    val hbConf = HBaseConfiguration.create()
    // 声明表的信息
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("cf"))//colomn family
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbConf.set(TableInputFormat.INPUT_TABLE, "table1")//table name
    hbConf.set(TableInputFormat.SCAN, scanToString)
    // 通过spark接口获取表中的数据
    val rdd = spark.sparkContext.newAPIHadoopRDD(hbConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
    // 遍历hbase table1表中的每一个partition, 然后更新到Hbase table2表
    // 如果数据条数较少，也可以使用rdd.foreach()方法
    rdd.foreachPartition(x => hBaseWriter(x, args(0)))
    spark.stop()
  }
  /**
    * 在exetutor端更新table2表记录
    *
    * @param iterator table1表的partition数据
    */
  def hBaseWriter(iterator: Iterator[(ImmutableBytesWritable, Result)], zkQuorum: String): Unit = {
    // 准备读取hbase
    val tableName = "table2"
    val columnFamily = "cf"
    val qualifier = "cid"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "24002")
    conf.set("hbase.zookeeper.quorum", zkQuorum)
    var table: Table = null
    var connection: Connection = null
    try {
      connection = ConnectionFactory.createConnection(conf)
      table = connection.getTable(TableName.valueOf(tableName))
      val iteratorArray = iterator.toArray
      val rowList = new util.ArrayList[Get]()
      for (row <- iteratorArray) {
        val get = new Get(row._2.getRow)
        rowList.add(get)
      }
      // 获取table2表记录
      val resultDataBuffer = table.get(rowList)
// 修改table2表记录
val putList = new util.ArrayList[Put]()
      for (i <- 0 until iteratorArray.size) {
        val resultData = resultDataBuffer(i) //hbase2 row
        if (!resultData.isEmpty) {
          // 查询hbase1Value
          var hbase1Value = ""
          val it = iteratorArray(i)._2.listCells().iterator()
          while (it.hasNext) {
            val c = it.next()
            // 判断cf和qualifile是否相同
            if (columnFamily.equals(Bytes.toString(CellUtil.cloneFamily(c)))
              && qualifier.equals(Bytes.toString(CellUtil.cloneQualifier(c)))) {
              hbase1Value = Bytes.toString(CellUtil.cloneValue(c))
            }
          }
          val hbase2Value = Bytes.toString(resultData.getValue(columnFamily.getBytes,
            qualifier.getBytes))
          val put = new Put(iteratorArray(i)._2.getRow)
          // 计算结果
          val resultValue = hbase1Value.toInt + hbase2Value.toInt
          // 设置结果到put对象
          put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
            Bytes.toBytes(resultValue.toString))
          putList.add(put)
        }
      }
      if (putList.size() > 0) {
        table.put(putList)
      }
    } catch {
      case e: IOException =>
        e.printStackTrace();
    } finally {
      if (table != null) {
        try {
          table.close()
        } catch {
          case e: IOException =>
            e.printStackTrace();
        }
      }
      if (connection != null) {
        try {
          //关闭Hbase连接
          connection.close()
        } catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    }
  }
  private def printUsage {
    System.out.println("Usage: {zkQuorum}")
    System.exit(1)
  }
}

