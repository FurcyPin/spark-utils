package fpin.spark.utils.testing

import java.io.File
import java.nio.file.Path
import java.util.UUID


object Utils {

  val tmp: String = System.getProperty("java.io.tmpdir")

  import java.io.IOException
  import java.nio.file.FileVisitResult
  import java.nio.file.Files
  import java.nio.file.SimpleFileVisitor
  import java.nio.file.attribute.BasicFileAttributes

  def recursiveDelete(directory: Path) = {
    Files.walkFileTree(directory,
      new SimpleFileVisitor[Path]() {

        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult =  { Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult =  {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
  }

  def withTmpFolder(block: File => Unit): Unit = {
    val uuid = UUID.randomUUID()
    val tmpDir = new File(s"$tmp/test/$uuid")
    val creationRes = tmpDir.mkdirs()
    try {
      block(tmpDir)
    } finally {
      val res = recursiveDelete(tmpDir.toPath)
    }
  }


}
