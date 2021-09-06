package QT.AdJob.project

/**
  * Project : AdJobF
  * Author  : zhilin.gao
  * Date    : 2021/9/2 17:38
  */

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class AdFAlg {

  def lackLn2doc(df: DataFrame): DataFrame = {

    val groupedDF: DataFrame = df.select(col("EQUIPMENTID"), col("MCID"), col("firstTimeDraw"), col("LINENMB"), col("CHECKPORT"), col("stdMdLnCnt"))
      .groupBy(col("EQUIPMENTID"), col("MCID"), col("firstTimeDraw"))
      .agg("CHECKPORT" -> "max", "stdMdLnCnt" -> "max")    //.as("CHECKPORT")
      //      .withColumnRenamed("count(LINENMB)", "LINENMB_COUNT")
      .withColumnRenamed("max(CHECKPORT)", "CHECKPORT")
      .withColumnRenamed("max(stdMdLnCnt)", "stdMdLnCnt")
      .withColumnRenamed("firstTimeDraw", "DATETIME")

    val lackdf2doc:DataFrame = groupedDF.withColumn("CODE", lit(2))
      .withColumn("DESCRIPTION", format_string("少线：【实际/标准 " + "%s/%s" + "】", col("CHECKPORT"), when(col("CHECKPORT") < col("stdMdLnCnt"), col("stdMdLnCnt")).otherwise(col("stdMdLnCnt") + 2)))
      .select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"), col("CODE"), col("DESCRIPTION"))


    lackdf2doc
  }


  def linkLnEstimate(df: DataFrame): DataFrame = {

    val w_len: org.apache.spark.sql.expressions.WindowSpec = Window.partitionBy(col("EQUIPMENTID"),
      col("MCID"), col("firstTimeDraw")).orderBy("LEN")

    val w_linenmb: org.apache.spark.sql.expressions.WindowSpec = Window.partitionBy(col("EQUIPMENTID"),
      col("MCID"), col("firstTimeDraw")).orderBy("LINENMB")

    val beyondlineestimate: DataFrame = df.withColumn("LEN", sqrt(pow(col("PADX") - col("LEADX"), 2) +
      pow(col("PADY") - col("LEADY"), 2)))
      .withColumn("LENRANK", rank().over(w_len))
      .filter(col("LENRANK") > lit(2))
      .withColumn("LINENMB", rank().over(w_linenmb))

    beyondlineestimate.select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"),
      col("firstTimeDraw"), col("LINENMB"), col("LEADX"), col("LEADY"),
      col("PADX"), col("PADY"), col("CHECKPORT"), col("PIECESINDEX"), col("MCID_#"), col("COUNT"))

  }


  def diff(df: DataFrame): DataFrame = {

    val w_len: org.apache.spark.sql.expressions.WindowSpec = Window.partitionBy(col("EQUIPMENTID"),
      col("MCID"), col("firstTimeDraw"), col("PIECESINDEX")).orderBy("LINENMB")

    val lagDF = df.withColumn("LEADX_LAG", lag("LEADX", 1).over(w_len))
      .withColumn("LEADY_LAG", lag("LEADY", 1).over(w_len))
      .withColumn("PADX_LAG", lag("PADX", 1).over(w_len))
      .withColumn("PADY_LAG", lag("PADY", 1).over(w_len))
      .withColumn("LEADLEN", sqrt(pow(col("LEADX") - col("LEADX_LAG"), 2) + pow(col("LEADY") - col("LEADY_LAG"), 2)))
      .withColumn("PADLEN", sqrt(pow(col("PADX") - col("PADX_LAG"), 2) + pow(col("PADY") - col("PADY_LAG"), 2)))
      .select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"), col("firstTimeDraw"), col("LINENMB"), col("LEADX"), col("LEADY"), col("PADX"), col("PADY"), col("LEADLEN"), col("PADLEN"), col("CHECKPORT"), col("PIECESINDEX"), col("MCID_#"))

    lagDF
  }


  def fullLnMkSta(df: DataFrame, stdmodelDF: DataFrame): DataFrame = {

    val stdmodDF: DataFrame = stdmodelDF.withColumnRenamed("lineNmb", "STDLINENMB")
      .withColumnRenamed("mcID", "STDMCID")
      .select(col("STDMCID"), col("STDLINENMB"), col("leadDiff"), col("padDiff"), col("leadThreshold"), col("padThreshold"))
    val fulllinestateDF: DataFrame = df.join(stdmodDF, (df.col("MCID_#")===stdmodDF.col("STDMCID")) && (df.col("LINENMB") === stdmodDF.col("STDLINENMB")),joinType = "left")
      .withColumn("LEADOFFSET", abs(col("LEADLEN") - col("leadDiff")))
      .withColumn("PADOFFSET", abs(col("PADLEN") - col("padDiff")))
      .withColumn("CODE", when((col("LEADOFFSET") > col("leadThreshold")) || (col("PADOFFSET") > col("padThreshold")), 1).otherwise(lit(0)))

    fulllinestateDF.select(col("EQUIPMENTID"), col("MCID"), col("DATETIME"), col("firstTimeDraw"), col("LINENMB"), col("LEADX"), col("LEADY"), col("PADX"), col("PADY"), col("LEADLEN"), col("PADLEN"), col("CHECKPORT"), col("PIECESINDEX"), col("MCID_#"), col("leadDiff"), col("padDiff"), col("leadThreshold"), col("padThreshold"), col("LEADOFFSET"), col("PADOFFSET"), col("CODE"))
  }


  def fullLn2doc(df: DataFrame): DataFrame = {

    val fullline2doc: DataFrame = df
      .withColumn("DESCRIPTION", when(col("CODE") === 1, format_string("lineN%sLeadOff%sPadOff%s", col("LINENMB"), format_number(col("LEADOFFSET"), 1), format_number(col("PADOFFSET"), 1))).otherwise("qualified"))
      .groupBy(col("EQUIPMENTID"), col("MCID"), col("firstTimeDraw")).agg(max("CODE").as("CODE"), concat_ws(";", collect_list(when(col("DESCRIPTION") === "qualified", null).otherwise(col("DESCRIPTION")))).as("DESCRIPTION"))
      .withColumn("DESCRIPTION", when(col("DESCRIPTION") === "", "qualified").otherwise(col("DESCRIPTION")))
      .withColumnRenamed("firstTimeDraw", "DATETIME")

    fullline2doc
  }

}
