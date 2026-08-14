name := "spark-hive-json-processing"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.scalaj"       %% "scalaj-http" % "2.4.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.6" % "provided",
  "org.scalatest"    %% "scalatest"  % "3.2.17" % Test
)

assembly / mainClass := Some("com.project.sparkhive.SparkHiveJsonPipeline")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
