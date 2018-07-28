package fpin.spark.utils.analysis

import org.apache.spark.sql.types.StructField

case class AnalysisResult(
  `Column Name`: String,
  `Type`: String,
  `Count`: Long,
  `Count Distinct`: Long,
  `Count Null`: Long,
  `Min`: String,
  `Max`: String,
  `Top 100 values`: List[(String, Long)]
) {

}

object AnalysisResult {

  private def format(a: Any): String = {
    a.toString.take(25).replace("\n", "")
  }

  def apply(colSChema: StructField, colAgg: ColumnAggregation) = {
    new AnalysisResult(
      `Column Name` = colSChema.name,
      `Type` = colSChema.dataType.simpleString,
      `Count` = colAgg.count,
      `Count Distinct` = colAgg.countDistinct ,
      `Count Null` = colAgg.countNull,
      `Min` = colAgg.min.map{format}.getOrElse("") ,
      `Max` = colAgg.max.map{format}.getOrElse(""),
      `Top 100 values` = colAgg.topN.map {
        case (c, v) if v == null => "null" -> c
        case (c, v) => format(v) -> c
      }
    )
  }

}

case class FormattedAnalysisResult(
  `Column Name`: String,
  `Type`: String,
  `Count`: Long,
  `Count Distinct`: Long,
  `Count Null`: Long,
  `Min`: String,
  `Max`: String,
  `Top 100 values`: String
) {

}

/**
  * Similar to AnalysisResult, except that the Top 100 Values
  * have been converted to string to ease for display.
  */
object FormattedAnalysisResult {

  private def format(a: Any): String = {
    a.toString.take(25).filter{
      c =>
        val i = c.toInt
        32 <= i && i <= 126 || 160 <= i && i <= 255
    }
  }

  def apply(colSChema: StructField, colAgg: ColumnAggregation) = {
    new FormattedAnalysisResult(
      `Column Name` = colSChema.name,
      `Type` = colSChema.dataType.simpleString,
      `Count` = colAgg.count,
      `Count Distinct` = colAgg.countDistinct ,
      `Count Null` = colAgg.countNull,
      `Min` = colAgg.min.map{format}.getOrElse("") ,
      `Max` = colAgg.max.map{format}.getOrElse(""),
      `Top 100 values` = colAgg.topN.map {
        case (c, v) if v == null => "null" -> c
        case (c, v) => format(v) -> c
      }.mkString("[", ", ", "]")
    )
  }

}
