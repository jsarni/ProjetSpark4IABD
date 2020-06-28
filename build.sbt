name := "projetspark4iabd"
version := "1.0"
scalaVersion := "2.12.11"
organization := "poc.prestacop"
libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.12" % "2.4.0",
    "org.apache.spark" % "spark-sql_2.12" % "2.4.0",
    "org.apache.spark" % "spark-streaming_2.12" % "2.4.0",
    "org.apache.kafka" %% "kafka" % "2.5.0",
    "com.typesafe" % "config" % "1.4.0",
    "org.jmockit" % "jmockit" % "1.34" % "test"
)