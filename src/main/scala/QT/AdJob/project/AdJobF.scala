package QT.AdJob.project

/**
  * Project : AdJobF
  * Author  : zhilin.gao
  * Date    : 2021/9/2 17:33
  */


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import java.util.Properties

import QT.AdJob.utils.SparkReader
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col


object AdJobF {

  val ss: SparkSession = SparkSession.builder()
    .appName("AdJobF")
    .master("local[*]")
    .config("spark.hadoop.validateOutputSpecs", "false")
    .config("spark.driver.extraJavaOptions", "-XX:PermSize=128M -XX:MaxPermSize=256M")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.network.timeout", "600s")
    .config("spark.port.maxRetries", "500")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.sql.shuffle.partitions", 200)
    //      .config("spark.scheduler.listenerbus.eventqueue.capacity", 100000)
    //      .config("spark.debug.maxToStringFields", 100)
    .getOrCreate()

  private val logger = LoggerFactory.getLogger(this.getClass)

  val sc: SparkContext = ss.sparkContext

  sc.setLogLevel("WARN")
  sc.hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
  sc.hadoopConfiguration.set("fs.hdfs.impl.disable.cache", "true")
  sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice")
  sc.hadoopConfiguration.set("dfs.nameservices", "nameservice")
  sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice", "bigdata01,bigdata02")
  sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata01", "10.170.3.11:8020")
  sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice.bigdata02", "10.170.3.12:8020")
  sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  logger.info("-------------------------------------------- alg start ----------------------------------------------\n")


  val properties = new Properties()
  val propertiesStream = this.getClass.getClassLoader.getResourceAsStream("adFresources.properties")
  properties.load(propertiesStream)
  val kuduMaster: String = properties.getProperty("kuduMaster")
  val tableName: String = properties.getProperty("KuduTableName")
  val equipmentPath: String = properties.getProperty("equipmentMapPath")
  val stdModelPathPrefix: String = properties.getProperty("stdModelPathPrefix")
  val mcIDPath: String = properties.getProperty("mcIDPath")
  val resStoreTableName: String = properties.getProperty("resStoreTableName")
  val nextRunDatePath: String = properties.getProperty("nextRunDatePath")
  val mailToUsers: Seq[String] = properties.getProperty("mailToUsers").split(",")
  val mailFromUser: String = properties.getProperty("mailFromUser")
  val mailFromPwd: String = properties.getProperty("mailFromPwd")
  val mailPort: Int = properties.getProperty("mailPort").toInt
  val mailHost: String = properties.getProperty("mailHost")
  val mailSubject: String = properties.getProperty("mailSubject")

  val kuduContext = new KuduContext(kuduMaster, sc)


  def main(args: Array[String]): Unit = {

    import ss.implicits._

    val stdModels = SparkReader.readByCsv(stdModelPathPrefix + "stdMcIDs.csv", ss)
//    print(stdModels.show(200))

    val stdMdLnCnt = stdModels.groupBy($"mcID").count()
      .withColumnRenamed("mcID", "stdmcID")
        .withColumnRenamed("count", "stdMdLnCnt")
//    print(stdMdLnCnt.show(50))

    val preDatetime = SparkReader.read(nextRunDatePath, ss)
    print(s"--------------------------------------- preDatetime: $preDatetime-------------------------------------------\n")

    val rawDF: DataFrame = SparkReader.getTrainData(kuduMaster, tableName, ss, preDatetime)
    //    rawDF.show(128, truncate=20)

    val nextRuntime: String = SparkReader.updateRuntime(ss, rawDF)

    print(s"--------------------------------------- nextRuntime: $nextRuntime-------------------------------------------\n")

    SparkReader.write(nextRuntime, nextRunDatePath)
    print("-------------------------------------------update datetime done!---------------------------------------------\n")

    val processedDF: DataFrame = AdFETL.processing(rawDF, stdMdLnCnt, ss).persist(StorageLevel.MEMORY_AND_DISK)

//    print(processedDF.show(200))

    val noProgDF: DataFrame = AdFETL.noProg(processedDF.where("stdMdLnCnt is null"))
//    print(noProgDF.show(200))

    val lackLnDa: DataFrame = processedDF.filter((col("CHECKPORT") < col("stdMdLnCnt")) || (col("CHECKPORT") === col("stdMdLnCnt") + 1))

    val fullLnDa: DataFrame = processedDF.filter((col("CHECKPORT") === col("stdMdLnCnt")) && (col("CHECKPORT") === col("COUNT")))

    val linkLnDa: DataFrame = processedDF.filter((col("CHECKPORT") > col("stdMdLnCnt") + 2) && (col("CHECKPORT") === col("COUNT")))

    val adfRunThis = new AdFAlg

    val lackLn2doc: DataFrame = adfRunThis.lackLn2doc(lackLnDa)

    val linkLn2FullLn: DataFrame = adfRunThis.linkLnEstimate(linkLnDa)

    val fullLnTtl: DataFrame = fullLnDa
        .select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"), col("firstTimeDraw"), col("LINENMB"), col("LEADX"), col("LEADY"), col("PADX"), col("PADY"), col("CHECKPORT"), col("PIECESINDEX"), col("MCID_#"), col("COUNT"))
        .union(linkLn2FullLn)
        .sort(col("EQUIPMENTID"), col("MCID"), col("firstTimeDraw"), col("PIECESINDEX"), col("LINENMB"))

    val fullLnDiff: DataFrame = adfRunThis.diff(fullLnTtl)
//    print(fullLnDiff.show(200))

    val fullLnSta: DataFrame = adfRunThis.fullLnMkSta(fullLnDiff, stdModels)
//    print(fullLnSta.show(200))

    val fullLn2doc: DataFrame = adfRunThis.fullLn2doc(fullLnSta)
//    print(fullLn2doc.show(200))

    val ttlLn: DataFrame = AdFETL.unionDataFrame(Seq(noProgDF, lackLn2doc, fullLn2doc)).persist()
    print(ttlLn.show(200))

    val ngLn: DataFrame = ttlLn.where("CODE != 0")
//    print(ngLn.show(200))

    ss.read.format("kudu")
      .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> resStoreTableName))
      .load
      .createOrReplaceTempView("ads_angle_rt_data_bak_tmp")

    val df = ss.sql(s"select * from ads_angle_rt_data_bak_tmp where `datetime` > '$preDatetime'")

    kuduContext.deleteRows(df, resStoreTableName)
    print("-------------------------------------------delete done!---------------------------------------------\n")

    SparkReader.writeRes2Kudu(ttlLn, resStoreTableName, ss)
    print("-------------------------------------------insert done!---------------------------------------------\n")

    processedDF.unpersist()
    ttlLn.unpersist()


    sc.stop()


  }

}
