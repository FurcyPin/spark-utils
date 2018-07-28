package fpin.spark.utils.analysis

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

/** A tree structure to store the number of leaves contained in the subtree.
  *
  * @param size Number of leaves
  * @param children List of children (recursive)
  */
case class SchemaSize(size: Int, children: Seq[SchemaSize]) {

  def withIndex(offset: Int = 0): SchemaSizeWithIndex = {
    val offsets = children.map{_.size}.scanLeft(offset){_ + _}
    val childrenWithIndices = children.zip(offsets).map{ case (c, i) => c.withIndex(i) }
    SchemaSizeWithIndex(offset, size, childrenWithIndices)
  }

}

/** A tree structure to store the column index and the number sub_fields contained in struct columns.
  *
  * @param index
  * @param size
  * @param children
  */
case class SchemaSizeWithIndex(index: Int, size: Int, children: Seq[SchemaSizeWithIndex]) {

}


object DataframeExpander {

  /**
    * Transform a dataframe schema into a new schema where all structs have been flattened.
    * The field names are kept, with a '.' separator for struct fields.
    *
    * @param schema A spark dataframe schema
    * @param explode If set, arrays are exploded and a '!' separator is appended to their name.
    * @return
    */
  def expandSchema(schema: StructType, explode: Boolean): StructType = {

    def flattenDataType(prefix: String, dataType: DataType, nullable: Boolean, metadata: Metadata): Seq[StructField] = {
      dataType match {
        case struct: StructType =>
          flattenStructType(struct, nullable, prefix + ".")
        case ArrayType(elementType, containsNull) if explode =>
          flattenDataType(prefix + "!", elementType, nullable || containsNull, metadata)
        case dataType: DataType =>
          StructField(prefix, dataType, nullable, metadata)::Nil
      }
    }

    def flattenStructType(schema: StructType, previousNullable: Boolean = false, prefix: String = ""): Seq[StructField] = {
      schema.flatMap {
        case StructField(name, subSchema: StructType, nullable, _) =>
          flattenStructType(subSchema, previousNullable || nullable, prefix + name + ".")
        case StructField(name, dataType, nullable, metadata) =>
          flattenDataType(prefix + name, dataType, previousNullable || nullable, metadata)
      }
    }

    StructType(flattenStructType(schema))
  }

  /* Compute for each field of the denormalized schema the number of fields in the sub-structure */
  private[analysis] def getSchemaSizes(schema: DataType, explode: Boolean): SchemaSize = {

    def rec(schema: DataType): SchemaSize = {
      schema match {
        case struct: StructType =>
          val res: Seq[SchemaSize] = struct.map { field => rec(field.dataType) }
          SchemaSize(res.map{_.size}.sum, res)
        case ArrayType(elementType, _) if explode => rec(elementType)
        case _ => SchemaSize(1, Nil)
      }
    }
    rec(schema)
  }

  def expandDataset[T](ds: Dataset[T]): Dataset[Row] = {
    val df = ds.toDF()
    def flatten(col: Any, schemaSize: SchemaSize ): Seq[Any] = {
      col match {
        case null =>
          List.fill(schemaSize.size)(null)
        case row: Row =>
          row.toSeq.zip(schemaSize.children).flatMap{(flatten _).tupled}
        case c =>
          c::Nil
      }
    }

    val denormalizedSchema: StructType = expandSchema(ds.schema, explode = false)
    val schemaSize: SchemaSize = getSchemaSizes(ds.schema, explode = false)

    val rdd = df.rdd.map{
      r =>
        val res = Row(flatten(r, schemaSize): _*)
        assert(
          res.size == denormalizedSchema.size,
          s"Wrong number of columns generated from row $r \n expected row of size ${denormalizedSchema.size} but got $res."
        )
        res
    }

    rdd.collect.foreach{println}
    df.sparkSession.createDataFrame(rdd, denormalizedSchema)
  }

}
