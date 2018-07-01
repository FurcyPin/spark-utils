





## Build 


```
sbt assembly
```

An assembly jar will be created at `target/scala-2.11/spark-utils-assembly-0.1.0-SNAPSHOT.jar`


## Use

* deploy the jar on the cluster as `programs/spark-utils.jar`

* launch a spark shell with `spark-shell --jars programs/spark-utils.jar`

```
import fpin.spark.utils.analysis.implicits._

val df = ???
val res = df.denormalize.analyze
res.show()


```
















