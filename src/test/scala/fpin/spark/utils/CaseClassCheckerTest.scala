package fpin.spark.utils

import java.io.File
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, SparkSession}
import org.scalatest.freespec.AnyFreeSpec

import scala.reflect.runtime.universe.TypeTag
import testing.Utils.withTmpFolder

import scala.util.control.NonFatal

class CaseClassCheckerTest extends AnyFreeSpec  {

  import TestCaseClasses._

  val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val spark = SparkSession.builder.config(sparkConf).getOrCreate()

  import spark.implicits._

  def toto[T1]()(implicit encoder1: Encoder[T1]): Unit = {

  }

  def testIsSubsetOf[T1, T2](shouldWork: Boolean)(
    implicit t1: TypeTag[T1], t2: TypeTag[T2],
    example1: T1, example2: T2,
    encoder1: Encoder[T1], encoder2: Encoder[T1]
  ): Unit = {
    assert(CaseClassChecker.isSubsetOf[T1, T2] === shouldWork)
    withTmpFolder { tmp =>
      val uuid = UUID.randomUUID()
      val outputPath = s"$tmp/output_$uuid"
      spark.createDataset(Seq(example1))(encoder1).write.parquet(outputPath)
      try {
        spark.read.parquet(outputPath).as(encoder2).collect()
      } catch {
        case NonFatal(e) =>
          sys.error(e.toString)
          e.printStackTrace()
      }
    }

  }

  "test run" in {

    CaseClassChecker.isSubsetOf[Small, Big]

    testIsSubsetOf[Big, Big](shouldWork = true)
    testIsSubsetOf[Small, Big](shouldWork = true)
    testIsSubsetOf[Big, Small](shouldWork = false)
    testIsSubsetOf[SwappedSmall, Big](shouldWork = true)
    testIsSubsetOf[Big, SwappedSmall](shouldWork = false)
    testIsSubsetOf[BadSmall, Big](shouldWork = false)
    testIsSubsetOf[Big, BadSmall](shouldWork = false)

    testIsSubsetOf[BiggerNestedBig, BiggerNestedBig](shouldWork = true)
    testIsSubsetOf[SmallerNestedSmall, BiggerNestedBig](shouldWork = true)
    testIsSubsetOf[SmallerNestedBig, BiggerNestedBig](shouldWork = true)
    testIsSubsetOf[BiggerNestedSmall, BiggerNestedBig](shouldWork = true)

    testIsSubsetOf[BiggerNestedBig, SmallerNestedSmall](shouldWork = false)
    testIsSubsetOf[SmallerNestedSmall, SmallerNestedSmall](shouldWork = true)
    testIsSubsetOf[SmallerNestedBig, SmallerNestedSmall](shouldWork = false)
    testIsSubsetOf[BiggerNestedSmall, SmallerNestedSmall](shouldWork = false)

    testIsSubsetOf[BiggerNestedBig, SmallerNestedBig](shouldWork = false)
    testIsSubsetOf[SmallerNestedSmall, SmallerNestedBig](shouldWork = true)
    testIsSubsetOf[SmallerNestedBig, SmallerNestedBig](shouldWork = true)
    testIsSubsetOf[BiggerNestedSmall, SmallerNestedBig](shouldWork = false)

    testIsSubsetOf[BiggerNestedBig, BiggerNestedSmall](shouldWork = false)
    testIsSubsetOf[SmallerNestedSmall, BiggerNestedSmall](shouldWork = true)
    testIsSubsetOf[SmallerNestedBig, BiggerNestedSmall](shouldWork = false)
    testIsSubsetOf[BiggerNestedSmall, BiggerNestedSmall](shouldWork = true)

   }

}

object TestCaseClasses {

  case class ExampleValue[T](value: T)

  case class Big(a: Int, b: Int, c: String)
  object Big {
    implicit val example: Big = Big(0, 1, "a")
  }

  case class Small(a: Int, b: Int)
  object Small {
    implicit val example: Small = Small(0, 1)
  }
  case class SwappedSmall(b: Int, a: Int)
  object SwappedSmall {
    implicit val example: SwappedSmall = SwappedSmall(1, 0)
  }
  case class BadSmall(a: Int, b:Int, c:Int)
  object BadSmall {
    implicit val example: BadSmall = BadSmall(0, 1, 2)
  }

  case class BiggerNestedBig(a: Int, nested: Big)
  object BiggerNestedBig {
    implicit val example: BiggerNestedBig = BiggerNestedBig(3, Big.example)
  }
  case class SmallerNestedSmall(nested: Small)
  object SmallerNestedSmall {
    implicit val example: SmallerNestedSmall = SmallerNestedSmall(Small.example)
  }
  case class SmallerNestedBig(nested: Big)
  object SmallerNestedBig {
    implicit val example: SmallerNestedBig = SmallerNestedBig(Big.example)
  }
  case class BiggerNestedSmall(a: Int, nested: Small)
  object BiggerNestedSmall {
    implicit val example: BiggerNestedSmall = BiggerNestedSmall(3, Small.example)
  }

}
