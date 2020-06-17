name := "projetspark4iabd"
version := "1.0"
scalaVersion := "2.11.12"
organization := "poc.prestacop"
libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.2.0",
    "org.apache.spark" % "spark-sql_2.11" % "2.2.0",
    "org.apache.spark" % "spark-streaming_2.11" % "2.2.0",
    "org.apache.kafka" %% "kafka" % "2.4.1",
    "com.typesafe" % "config" % "1.4.0",
    "org.jmockit" % "jmockit" % "1.34" % "test"
)