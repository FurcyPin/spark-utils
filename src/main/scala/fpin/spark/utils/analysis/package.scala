package fpin.spark.utils

import org.apache.spark.sql.{DataFrame, Dataset, Row}

package object analysis {

  type ColumnId = Int
  type Value = Any

  object implicits {

    implicit class DataSetExtension[T](ds: Dataset[T]) {

      def analyze(explode: Boolean = true): DataFrame = {
        DatasetAnalyzer.analyze(ds, explode)
      }

      def expand(): Dataset[Row] = {
        DataframeExpander.expandDataset(ds)
      }

    }

  }

}
