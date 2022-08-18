

organization := "fpin"
name := "spark-utils"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"

scalacOptions in Compile ++= Seq("-unchecked",  "-deprecation",  "-feature")

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

// libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.5.0"

// libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

val spark_version = "3.1.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % spark_version % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "provided"


libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version % "test" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-sql"  % spark_version % "test" withSources() withJavadoc()
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test" withSources() withJavadoc()


libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.13" % "test"

parallelExecution in Test := false

javaOptions in Test += "-XX:MaxPermSize=1G -XX:MaxMetaspaceSize=1G"


