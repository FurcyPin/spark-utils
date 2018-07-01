package fpin.spark.utils.analysis

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

object Denormalizer {

  def denormalizeSchema(schema: StructType): StructType = {
    def flatten(schema: StructType, prefix: String = ""): Seq[StructField] = {
      schema.flatMap {
        case StructField(name, subSchema: StructType, nullable, metadata) =>
          flatten(subSchema, name + ".")
        case StructField(name, dataType, nullable, metadata) =>
          StructField(prefix + name, dataType, nullable, metadata)::Nil
      }
    }
    StructType(flatten(schema))
  }

  def denormalize[T](ds: Dataset[T]): Dataset[Row] = {
    val df = ds.toDF()
    def flatten(row: Row): Seq[Any] = {
      row.toSeq.flatMap {
        case subRow: Row => flatten(subRow)
        case c => c::Nil
      }
    }
    val rdd = df.rdd.map{r => Row(flatten(r): _*)}

    df.sparkSession.createDataFrame(rdd, denormalizeSchema(ds.schema))
  }

}
