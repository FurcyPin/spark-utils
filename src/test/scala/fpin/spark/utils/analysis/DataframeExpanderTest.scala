package fpin.spark.utils.analysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FreeSpec

object DataframeExpanderTest {

  case class Struct(A: Option[Int], B: Boolean, C: Long)

  case class StructStruct(D: Long, F: Option[Struct], G: Option[Struct], H: Long)

  case class Data(I: Option[StructStruct])

  case class ArrayData(J: Option[Array[Int]])

  case class ArrayStructData(K: Int, L: Option[Array[Struct]], M: Int)

}

class DataframeExpanderTest extends FreeSpec {

  import DataframeExpanderTest._

  val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

  val spark = SparkSession.builder.config(sparkConf).getOrCreate()

  import spark.implicits._

  "expand should work with null structs" in {

    val b = Struct(None, true, 3l)
    val c = StructStruct(3l, None, None, 3l)
    val d = Data(Some(c))

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._

    val actual: Seq[Any] = ds.expand().collect.head.toSeq

    val expected = Seq(3, null, null, null, null, null, null, 3l)

    assert(actual === expected)
  }

  "expand should work with null nested structs" in {

    val d = Data(None)

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._
    val actual: Seq[Any] = ds.expand().collect.head.toSeq
    val expected = Seq(null, null, null, null, null, null, null, null)

    assert(actual === expected)
  }

  "expand(explode = true) should work with struct arrays" in {

    val d = ArrayData(Some(Array(1, 2)))

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._

    ds.expand().printSchema
//    val actual: Seq[Any] = ds.expand().collect.head.toSeq
//    actual.foreach{println}
//    val expected = Seq(null, null, null, null, null, null, null, null)
//    actual.foreach{println}

//    assert(actual === expected)
  }

  "expand should work with arrays of structs" in {
    val b = Struct(None, true, 3l)
    val c = Struct(Some(1), false, 2l)
    val d = ArrayStructData(5, Some(Array(b, c)), 6)

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._
    ds.expand().printSchema()


    //    val actual: Seq[Any] = ds.expand().collect.head.toSeq
    //    val expected = Seq(null, null, null, null, null, null, null, null)
    //    actual.foreach{println}

    //    assert(actual === expected)
  }

  "expandSchema(explode = false) should work with arrays of structs" in {
    val b = Struct(None, true, 3l)
    val c = Struct(Some(1), false, 2l)
    val d = ArrayStructData(5, Some(Array(b, c)), 6)

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._
    println(DataframeExpander.expandSchema(ds.schema, explode = false))


    //    val actual: Seq[Any] = ds.expand().collect.head.toSeq
    //    val expected = Seq(null, null, null, null, null, null, null, null)
    //    actual.foreach{println}

    //    assert(actual === expected)
  }

  "expandSchema(explode = true) should work with arrays of structs" in {
    val b = Struct(None, true, 3l)
    val c = Struct(Some(1), false, 2l)
    val d = ArrayStructData(5, Some(Array(b, c)), 6)

    val ds = spark.createDataset(Seq(d, d, d))

    import fpin.spark.utils.analysis.implicits._
    println(DataframeExpander.expandSchema(ds.schema, explode = true))


    //    val actual: Seq[Any] = ds.expand().collect.head.toSeq
    //    val expected = Seq(null, null, null, null, null, null, null, null)
    //    actual.foreach{println}

    //    assert(actual === expected)
  }

  "getSchemaSizes should work" in {
    val b = Struct(None, true, 3l)
    val c = StructStruct(3l, None, Some(b), 3l)
    val d = Data(Some(c))

    val ds = spark.createDataset(Seq(d, d, d))

    val actual = DataframeExpander.getSchemaSizes(ds.schema, explode = false)
    val expected =
      SchemaSize(8, List(
        SchemaSize(8, List(
          SchemaSize(1, Nil),
          SchemaSize(3, List(
            SchemaSize(1, Nil),
            SchemaSize(1, Nil),
            SchemaSize(1, Nil)
          )),
          SchemaSize(3, List(
            SchemaSize(1, Nil),
            SchemaSize(1, Nil),
            SchemaSize(1, Nil)
          )),
          SchemaSize(1, Nil)
        ))
      ))
    assert(actual === expected)
  }

  "getSchemaSizeWithIndex should work" in {
    val b = Struct(None, true, 3l)
    val c = StructStruct(3l, None, Some(b), 3l)
    val d = Data(Some(c))

    val ds = spark.createDataset(Seq(d, d, d))

    val actual = DataframeExpander.getSchemaSizes(ds.schema, explode = false).withIndex()
    val expected =
      SchemaSizeWithIndex(0, 8, List(
        SchemaSizeWithIndex(0, 8, List(
          SchemaSizeWithIndex(0, 1, Nil),
          SchemaSizeWithIndex(1, 3, List(
            SchemaSizeWithIndex(1, 1, Nil),
            SchemaSizeWithIndex(2, 1, Nil),
            SchemaSizeWithIndex(3, 1, Nil)
          )),
          SchemaSizeWithIndex(4, 3, List(
            SchemaSizeWithIndex(4, 1, Nil),
            SchemaSizeWithIndex(5, 1, Nil),
            SchemaSizeWithIndex(6, 1, Nil)
          )),
          SchemaSizeWithIndex(7, 1, Nil)
        ))
      ))
    assert(actual === expected)
  }

  "getSchemaSizes(ds, explode = true) should work with an array" in {
    val d = ArrayData(Some(Array(1, 2)))

    val ds = spark.createDataset(Seq(d, d, d))

    val actual = DataframeExpander.getSchemaSizes(ds.schema, explode = true)
    val expected = SchemaSize(1,List(SchemaSize(1,List())))
    assert(actual === expected)
  }

  "getSchemaSizes(ds, explode = true) should work with an array of structs" in {
    val b = Struct(None, true, 3l)
    val c = Struct(Some(1), false, 2l)
    val d = ArrayStructData(5, Some(Array(b, c)), 6)

    val ds = spark.createDataset(Seq(d, d, d))

    val actual = DataframeExpander.getSchemaSizes(ds.schema, explode = true)
    ds.printSchema()
    val expected = SchemaSize(5,List(SchemaSize(1,List()), SchemaSize(3,List(SchemaSize(1,List()), SchemaSize(1,List()), SchemaSize(1,List()))), SchemaSize(1,List())))
    assert(actual === expected)
  }

  "getSchemaSizes(ds, explode = false) should work with an array of structs" in {
    val b = Struct(None, true, 3l)
    val c = Struct(Some(1), false, 2l)
    val d = ArrayStructData(5, Some(Array(b, c)), 6)

    val ds = spark.createDataset(Seq(d, d, d))

    val actual = DataframeExpander.getSchemaSizes(ds.schema, explode = false)
    val expected = SchemaSize(3,List(SchemaSize(1,List()), SchemaSize(1,List()), SchemaSize(1,List())))
    assert(actual === expected)
  }

}


