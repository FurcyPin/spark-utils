package fpin.spark.utils.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.freespec.AnyFreeSpec

object MultiAnalyzerTest {

  case class Data(
    intCol: Int,
    stringCol: String,
    booleanCol: Boolean,
    nullableIntCol: Option[Int],
    nullableStringCol: Option[String],
    byteArrayCol: Option[Array[Byte]] = None
  )

  val data: Seq[Data] =
    Seq(
      Data(1,  "a", true, Some(1), Some("a")),
      Data(2,  "b", false, Some(2), Some("b")),
      Data(3,  "c", true, None, Some("c")),
      Data(4,  "a", false, Some(4), Some("a")),
      Data(5,  "b", true, Some(5), None),
      Data(6,  "c", false, None, Some("c")),
      Data(7,  "a", true, Some(7), Some("a")),
      Data(8,  "b", false, Some(8), Some("b")),
      Data(9,  "c", true, None, Some("c")),
      Data(10, "a", false, Some(10), None),
      Data(11, "b", true, Some(11), Some("b")),
      Data(12, "c", false, None, Some("c")),
      Data(13, "a", true, Some(13), Some("a")),
      Data(14, "b", false, Some(14), Some("b")),
      Data(15, "c", true, None, None),
      Data(16, "a", false, Some(16), Some("a")),
      Data(17, "b", true, Some(17), Some("b"), byteArrayCol = Some("abc".getBytes)),
      Data(18, "c", false, None, Some("c"), byteArrayCol = Some("abc".getBytes))
    )

}

case class Data(
  `a.b`: Int
)


class MultiAnalyzerTest extends AnyFreeSpec {

  implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[4]").getOrCreate()

  "analyze should work" in {
    import spark.implicits._
    val dataset: (String, DataFrame) = "test" -> spark.createDataset(MultiAnalyzerTest.data).toDF()
    val res = MultiAnalyzer.analyze(dataset::Nil)
    println(res)
  }

}

