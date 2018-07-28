package fpin.spark.utils.analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object DatasetAnalyzer {

  /**
    *
    * @param ds
    * @param explode
    * @tparam T
    * @return
    */
  def analyze[T](ds: Dataset[T], explode: Boolean): DataFrame = {
    import ds.sparkSession.implicits._

    def flatten(col: Any, schemaSize: SchemaSizeWithIndex): Seq[(ColumnId, Any)] = {
      col match {
        case null =>
          List.fill(schemaSize.size)(null).zipWithIndex.map{case (null, i) => (schemaSize.index + i, null)}
        case row: Row =>
          row.toSeq.zip(schemaSize.children).flatMap{case (c, s) => flatten(c, s)}
        case array: Iterable[Any] if explode =>
          array.iterator.flatMap(flatten(_, schemaSize)).toSeq
        case c =>
          (schemaSize.index, c)::Nil
      }
    }

    val schemaSize: SchemaSizeWithIndex = DataframeExpander.getSchemaSizes(ds.schema, explode).withIndex()
    val schemaPerColId: Map[ColumnId, StructField] = DataframeExpander.expandSchema(ds.schema, explode).zipWithIndex.map{case (f, i) => i -> f}.toMap

    val columnValueCountsRDD: RDD[((ColumnId, Value), Long)] =
      ds.toDF().repartition(numPartitions = math.max(ds.rdd.getNumPartitions, 20)).rdd
        .flatMap{flatten(_, schemaSize)}
        .map{(_, 1L)}
        .reduceByKey{_ + _}

    columnValueCountsRDD
      .map {
        case ((colId, value), counter) =>
          colId -> ColumnAggregation(value, counter)
      }
      .reduceByKey{ _ merge _ }
      .sortByKey()
      .map {
        case (colId, colAgg) => FormattedAnalysisResult(schemaPerColId(colId), colAgg)
      }.toDF()
  }

}
