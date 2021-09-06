package QT.AdJob.project

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Project : AdJobF
  * Author  : zhilin.gao
  * Date    : 2021/9/2 17:39
  */
object AdFETL {

  def processing(rawtraindata: DataFrame, stdMdLnCnt: DataFrame, ss: SparkSession): DataFrame = {

    import ss.implicits._
    //    val spec = Seq("EQUIPMENTID", "MCID", "PIECESINDEX")
    val w = Window.partitionBy($"EQUIPMENTID", $"MCID", $"PIECESINDEX")
    val df = rawtraindata.withColumn("firstTimeDraw", min($"DATETIME").over(w))
      .withColumn("MCID_#", split($"MCID", "#").getItem(0))
      .withColumn("COUNT",count("LINENMB").over(w))

    val mergeDF = df.join(stdMdLnCnt, $"MCID_#"===$"stdmcID", "left")  //($"MCID"===$"stdmcID" && $""===$"")
//    df.join(stdMdLnCnt, df.col("MCID").equalTo(stdMdLnCnt.col("stdmcID").and(df.col("").equalTo(stdMdLnCnt.col(""))))).where("MCID is null")

    mergeDF.select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"), col("firstTimeDraw"),
      col("LINENMB"), col("LEADX"), col("LEADY"), col("PADX"),
      col("PADY"), col("CHECKPORT"), col("PIECESINDEX"), col("MCID_#"), col("COUNT"), col("stdMdLnCnt"))
      .sort(col("EQUIPMENTID"), col("MCID"), col("firstTimeDraw"), col("LINENMB"))
  }


  def unionDataFrame(seq: Seq[DataFrame]): DataFrame = {

    var firstDF = seq.head

    for (idx <- 1 until seq.length) {
      firstDF = firstDF.union(seq(idx))
    }

    firstDF
  }


  def noProg(df: DataFrame): DataFrame = {

    val noProgDF: DataFrame = df.select(col("EQUIPMENTID"), col("MCID"), col("DATETIME")).groupBy("EQUIPMENTID", "MCID")
      .agg("DATETIME" -> "min").alias("DATETIME")
      .withColumnRenamed("min(DATETIME)", "DATETIME")
      .withColumn("CODE", lit(3))
      .withColumn("DESCRIPTION", lit("NO PROGRAM"))
    //      .drop(col("min(DATETIME)"))

    noProgDF
  }

}
