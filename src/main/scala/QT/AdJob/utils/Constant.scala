package QT.AdJob.utils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Project : AdJobF
  * Author  : zhilin.gao
  * Date    : 2021/9/2 17:45
  */
object Constant {

  final val IMPALA_JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver"
  final val IMPALA_CONNECTION_URL = "jdbc:impala://10.170.3.12:21050/erp_job;UseSasl=0;AuthMech=3;UID=qtkj;PWD=qt_qt"
  final val ORACLE_JDBC_DRIVER = "oracle.jdbc.OracleDriver"
  final val KUDU_MASTER = "bigdata01,bigdata02,bigdata03"
  final val HDFS_EQPATH = "/jobs/spark/angleStdModels/equipmentMap.csv"
  final val STDMODELPATHPREFIX = "/jobs/spark/angleStdModels/"
  final val PRERUNDATEPATH = "/jobs/spark/angleStdModels/preRunDate.txt"
  final val MCIDPATH = "/jobs/spark/angleStdModels/mcID.csv"
  final val RESULTTABLE = "ADS_ANGLE_RT_DATA_BAK"
  final val KUDU_TABLE = "ADS_ANGLE_RT_DATA"
  final val scheam = StructType(
    StructField("EQUIPMENTID", StringType, false) ::
      StructField("MCID", StringType, false) ::
      StructField("DATETIME", StringType, false) ::
      StructField("CODE", IntegerType, true) ::
      StructField("DESCRIPTION", StringType, true):: Nil)
  final val MAILUSER = "bigdata.it@qtechglobal.com"
  final val MAILPWD = "qtech2020"
  final val mailToUsers = "zhilin.gao@qtechglobal.com,zhilin.gao@qtechglobal.com"
  final val mailHost = "123.58.177.49"
  final val mailPort = "25"


}
