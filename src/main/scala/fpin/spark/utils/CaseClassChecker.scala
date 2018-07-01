package fpin.spark.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import scala.reflect.runtime.universe.TypeTag

object CaseClassChecker {

  private def isSubsetOf (t1: DataType, t2: DataType): Boolean = {
    (t1, t2) match {
      case (StructType(fields1), StructType(fields2)) =>
        fields1.forall{
          case StructField(name1, type1, _, _) =>
            fields2.exists{ s => name1 == s.name && isSubsetOf(type1, s.dataType) }
        }
      case (type1, type2) => type1.typeName == type2.typeName
    }
  }

  /** Compare two case class types to see if the first one matches a subset of the second one.
    *
    * When true, it means that a case class of the first type can be safely used to read
    * a Parquet input represented by the second type.
    */
  def isSubsetOf[T1: TypeTag, T2: TypeTag]: Boolean = {
    val schema1: DataType = ScalaReflection.schemaFor[T1].dataType
    val schema2: DataType = ScalaReflection.schemaFor[T2].dataType
    isSubsetOf(schema1, schema2)
  }

}
