ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "0.1.1"
ThisBuild / organization := "com.iomete"

val sparkVersion = "3.2.1"

resolvers ++= Seq(
  "confluent" at "https://packages.confluent.io/maven/",
  Resolver.mavenLocal //so we can use local build of kafka-avro-serializer
)

lazy val root = (project in file(".")).
  settings(
    name := "kafka-streaming-job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
      "io.confluent" % "kafka-avro-serializer" % "6.0.0"
    ),

    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
  )

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}