package fpin.spark.utils.analysis

import java.io.{FileOutputStream, PrintStream}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}


/** Use this to recursively scan a whole hdfs folder
  * and perform an automatic analysis on every dataset
  * found inside.
  *
  */
object HdfsFolderAnalyzer {

  implicit class FileStatusExtension(f: FileStatus) {
    def isHidden: Boolean = Seq('_', '.').contains{f.getPath.getName.charAt(0)}
  }

  private def listSubDirectories(path: Path, fs: FileSystem): Vector[Path] = {
    def aux(path: Path): Vector[Path] = {
      val (subDirs, subFiles) = fs.listStatus(path).filterNot{_.isHidden}.partition{_.isDirectory}
      if(subDirs.isEmpty && subFiles.nonEmpty) {
        Vector(path)
      }
      else {
        subDirs.toVector.flatMap{f => aux(f.getPath)}
      }
    }
    aux(path)
  }

  private def getFileExtensionsInFolder(path: Path, fs: FileSystem): Set[String] = {
    val subFiles = fs.listStatus(path).iterator.filterNot{_.isHidden}.filterNot{_.isDirectory}
    val extensions: Set[String] = subFiles.map{_.getPath.getName.split("[.]").last}.toSet
    extensions
  }

  def analyzeFolder(string: String)(implicit spark: SparkSession): String = {
    analyzeFolder(new Path(string))
  }

  def analyzeFolder(path: Path, explode: Boolean = true)(implicit spark: SparkSession): String = {
    import fpin.spark.utils.analysis.implicits._

    val fs: FileSystem = FileSystem.get(path.toUri, spark.sparkContext.hadoopConfiguration)
    val folders: Seq[(Path, Set[String])] =
      listSubDirectories(path, fs).map{
        f => f -> getFileExtensionsInFolder(f, fs)
      }

    val dataFrames: Iterator[(String, DataFrame)] =
      folders.iterator.flatMap {
        case (folder: Path, extensions: Set[String]) if extensions == Set("parquet") =>
          (folder.toString -> spark.read.parquet(folder.toString))::Nil
        case (folder: Path, extensions: Set[String]) if extensions == Set("orc") =>
          (folder.toString -> spark.read.orc(folder.toString))::Nil
        case (folder: Path, extensions: Set[String]) =>
          println()
          println(folder)
          println("inconsistent file formats found: " + extensions.map{"." + _}.mkString("(", ", ", ")"))
          Nil
      }

    MultiAnalyzer.analyze(dataFrames.toTraversable)
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

  def main(args: Array[String]): Unit = {
    if(args.isEmpty) {
      println("Please specify a folder to scan")
    }
    else {
      val spark = SparkSession.builder().appName("Datalyzer").getOrCreate()
      val result = HdfsFolderAnalyzer.analyzeFolder(args(0))(spark)
      appendToFile(result, "report.csv")
    }
  }

}

