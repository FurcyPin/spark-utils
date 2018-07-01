package fpin.spark.utils

import fpin.spark.utils.analysis.Denormalizer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FreeSpec

object DenormalizerTest {

  case class ArrayString(email_md5: Array[String], login_md5: Array[String], email_md5_sha256: Array[String])

  case class Data(xci: ArrayString)

}

class DenormalizerTest extends FreeSpec {

  import DenormalizerTest._

  val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

  val spark = SparkSession.builder.config(sparkConf).getOrCreate()

  import spark.implicits._

  "test" ignore {

    val a = ArrayString(Array("a", "b"), Array("c", "d"), Array("e", "f"))
    val d = Data(a)

    val ds = spark.createDataset(Seq(d, d, d))

    ds.printSchema()

    Denormalizer.denormalize(ds).collect.foreach{println}

  }

}

//root
//|-- xci: struct (nullable = true)
//|    |-- email_md5: array (nullable = true)
//|    |    |-- element: string (containsNull = true)
//|    |-- login_md5: array (nullable = true)
//|    |    |-- element: string (containsNull = true)
//|    |-- email_md5_sha256: array (nullable = true)
//|    |    |-- element: string (containsNull = true)


//root
//|-- xci: struct (nullable = true)
//|    |-- email_md5: array (nullable = true)
//|    |    |-- element: string (containsNull = true)
//|    |-- login_md5: array (nullable = true)
//|    |    |-- element: string (containsNull = true)
//|    |-- email_md5_sha256: array (nullable = true)
//|    |    |-- element: string (containsNull = true)

