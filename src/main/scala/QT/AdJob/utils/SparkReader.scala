package QT.AdJob.utils

import java.io.{IOException, OutputStream}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Calendar
import QT.AdJob.utils.Constant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, max}

import scala.collection.immutable.Map

/**
  * Project : AdJobF
  * Author  : zhilin.gao
  * Date    : 2021/9/2 17:44
  */
object SparkReader {

  def getConf = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new Configuration
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("fs.defaultFS", "hdfs://nameservice")
    conf.set("dfs.nameservices", "nameservice")
    conf.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
    conf.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
    conf.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    conf
  }


  @throws[IOException]
  def write(time: String, path: String): Unit = {

    val cal = Calendar.getInstance
    cal.add(Calendar.MINUTE, 0)

    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(getConf)
    try {
      val output: OutputStream = fileSystem.create(new Path(path))

      output.write(time.getBytes("UTF-8"))

      fileSystem.close()
      output.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }


  def readByCsv(path: String, spark: SparkSession): DataFrame = {

    var fileSystem: FileSystem = null
    fileSystem = FileSystem.get(getConf)

    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(path)
    df
  }


  def read(readPath: String, spark: SparkSession): String = {
    val lines = spark.read.textFile(readPath).rdd.collect()

    val datetime = lines.head.replace("T", " ")

    datetime
  }


  def getTrainData(kuduMaster: String, tableName: String, ss: SparkSession, filterStr: String): DataFrame = {
    val trainDF = ss.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName)).format("kudu").load
    trainDF.createOrReplaceTempView("kuduTable")
    val filterCmd = s"select * from kuduTable where linenmb > 0 and datetime > '$filterStr'"
    val selectDF = ss.sql(filterCmd)

    selectDF
  }


  def updateRuntime(spark: SparkSession, rawDF: DataFrame): String = {

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val maxDateTime = rawDF.select(max(col("DATETIME")).alias("DATETIME")).collect().slice(0, 1)(0)(0).toString

    val toDatetime = LocalDateTime.parse(maxDateTime, formatter)

    val nextRuntime = toDatetime.minusMinutes(-20)

    val dtf = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    val dateTime2Str: String = dtf.format(toDatetime).replace("T", " ")

    dateTime2Str
  }


  def writeRes2Kudu(df: DataFrame, tableName: String, ss: SparkSession): Unit = {

    ss.createDataFrame(df.rdd, Constant.scheam).write
      .format("org.apache.kudu.spark.kudu")
      .mode(SaveMode.Append)
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", tableName)
      .save()
  }
}


