package fpin.spark.utils.analysis

import java.io.{FileOutputStream, PrintStream}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.matching.Regex


/** Use this to scan a whole hive database
  * and perform an automatic analysis on every table
  * found inside.
  *
  */
object HiveAnalyzer {

  case class Table(name: String, database: String, description: String, tableType: String, isTemporary: Boolean) {

    def fullTableName: String = {
      s"$database.$name"
    }

  }

  def appendToFile(content: String, file: String): Unit = {
    val fos = new FileOutputStream(file, true)
    val ps = new PrintStream(fos)
    try {
      ps.println(content)
    } finally {
      ps.close()
      fos.close()
    }
  }

  def analyzeTables(dbName: String, regexFilters: Regex*)(implicit spark: SparkSession): String = {
    import spark.implicits._
    val dataframes: TraversableOnce[(String, DataFrame)] =
      for {
        t:Table <- spark.catalog.listTables(dbName).as[Table].collect().iterator
        if regexFilters.forall{r => r.pattern.matcher(t.name).matches()}
      } yield {
        t.fullTableName -> spark.table(t.fullTableName)
      }
    MultiAnalyzer.analyze(dataframes.toTraversable)
  }

  def main(args: Array[String]): Unit = {
    if(args.isEmpty) {
      println("Please specify a schema name to analyze")
    }
    else {
      val spark = SparkSession.builder().appName("Datalyzer").enableHiveSupport().getOrCreate()
      val res = HiveAnalyzer.analyzeTables(args(0), args.tail.map{_.r}:_*)(spark)
      appendToFile(res, s"${args(0)}.report.csv")
      spark.stop()
    }
  }

}
