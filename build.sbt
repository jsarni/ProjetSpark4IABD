lazy val commonSettings = Seq(
    organization := "poc.prestacop",
    version := "1.0",
    scalaVersion := "2.12.11",
    libraryDependencies ++= Seq(
        "com.typesafe" % "config" % "1.4.0",
        "org.jmockit" % "jmockit" % "1.34" % "test",
        "org.slf4j" % "slf4j-api" % "1.7.25",
        "org.apache.spark" % "spark-core_2.12" % "2.4.0",
        "org.apache.spark" % "spark-sql_2.12" % "2.4.0",
    )
)

lazy val Project = (project in file("."))
  .settings(
      commonSettings,
  )
  .aggregate(
      Commons,
      HistoricDataLoader,
      HistoricDataSaver,
      DataProcessor,
      MessageProducer,
      MessageConsumer,
  )

lazy val Commons = (project in file("Commons"))
  .settings(
      name := "Commons",
      commonSettings,
      libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % "2.5.0",
        "com.typesafe.play" %% "play-json" % "2.9.0",
      )
  )

lazy val HistoricDataLoader = (project in file("HistoricDataLoader"))
  .settings(
      name := "HistoricDataLoader",
      commonSettings,
      libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % "2.5.0",
      )
  ).dependsOn(Commons)

lazy val HistoricDataSaver = (project in file("HistoricDataSaver"))
  .settings(
      name := "HistoricDataSaver",
      commonSettings,
      libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % "2.5.0",
      )
  ).dependsOn(Commons)

lazy val DataProcessor = (project in file("DataProcessor"))
  .settings(
      name := "DataProcessor",
      commonSettings,
  ).dependsOn(Commons)

lazy val MessageProducer = (project in file("MessageProducer"))
  .settings(
      name := "MessageProducer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.5.0",
      "com.typesafe.play" %% "play-json" % "2.9.0",
    )
  ).dependsOn(Commons)

lazy val MessageConsumer = (project in file("MessageConsumer"))
  .settings(
    name := "MessageConsumer",
    commonSettings,
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.5.0",
      "com.typesafe.play" %% "play-json" % "2.9.0",
      "org.apache.commons" % "commons-email" % "1.5",
    )
  ).dependsOn(Commons)