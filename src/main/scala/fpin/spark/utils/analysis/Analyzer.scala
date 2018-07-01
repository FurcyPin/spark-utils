package fpin.spark.utils.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

object Analyzer {

  case class AnalysisResult(
    `Column Name`: String,
    `Count Distinct`: Long,
    `Top 100 values`: List[(Long, String)]
  ) {

  }

  def analyze[T](ds: Dataset[T]): DataFrame = {
    import ds.sparkSession.implicits._

    val columnIdsToName: Map[ColumnId, String] = ds.schema.zipWithIndex.map{case (f, i) => i -> f.name}.toMap
    val columnValueCountsRDD: RDD[((ColumnId, Value), Long)] =
      ds.toDF().rdd
        .flatMap {
          row =>
            row.toSeq.zipWithIndex.map{
              case (value, i) => (i, value) -> 1L
            }
        }
        .reduceByKey{_ + _}

    columnValueCountsRDD
      .map {
        case ((colId, value), counter) =>
          colId -> ColumnAggregation(value, counter)
      }
      .reduceByKey{ _ merge _ }
      .sortByKey()
      .map {
        case (colId, colAgg) =>
          AnalysisResult(
            `Column Name` = columnIdsToName(colId),
            `Count Distinct` = colAgg.nbDistinct ,
            `Top 100 values` = colAgg.topN.map {
              case (c, v) if v == null => c -> "null"
              case (c, v) => c -> v.toString
            }
          )
      }.toDF()
  }

}
