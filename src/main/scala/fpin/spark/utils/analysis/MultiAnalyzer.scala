package fpin.spark.utils.analysis

import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object MultiAnalyzer {

  def analyze(dataframes: Traversable[(String, DataFrame)])(implicit spark: SparkSession): String = {
    val sb = new StringBuilder
    for {
      (name, df) <- dataframes
    } {
      sb ++= name
      sb ++= "\n"
      import fpin.spark.utils.analysis.implicits._
      val res: DataFrame = df.analyze(explode = true)
      sb ++= res.schema.map{_.name}.mkString("|")
      sb ++= "\n"
      res.collect().foreach{
        row: Row =>
          sb ++= row.toSeq.map{_.toString}.mkString("|")
          sb ++= "\n"
      }
      sb ++= "\n"
    }
    sb.toString()
  }

}
