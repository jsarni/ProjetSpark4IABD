package poc.prestacop.HistoricDataSaver

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import poc.prestacop.Commons.AppConfig

import scala.util.Try
import java.util.Collections._
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Append
import scala.jdk.CollectionConverters._

class HistoricDataSaver(spark: SparkSession) {

    import HistoricDataSaver._

    val mainConsumerTry: Try[KafkaConsumer[String, String]] = Try {
        val mainKafkaProps: Properties = new Properties()

        mainKafkaProps.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        mainKafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        mainKafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        mainKafkaProps.put("group.id", KAFKA_MAIN_CONSUMERS_GROUP_ID)


        new KafkaConsumer[String, String](mainKafkaProps)

    }
    def run(): Unit = {
        for {
            mainConsumer <- mainConsumerTry
            _ = mainConsumer.subscribe(singletonList(MAIN_KAFKA_TOPIC))
            _ = manageFilesToProcess(mainConsumer)
            _ = closeKafkaConsumer(mainConsumer, KAFKA_MAIN_CONSUMER_CLOSE_DURATION_MUNITES)
        } yield ()
    }

    @scala.annotation.tailrec
    private[this] def manageFilesToProcess(kafkaConsumer: KafkaConsumer[String, String]): Unit = {
        val records: ConsumerRecords[String, String] =
            kafkaConsumer.poll(Duration.ofMinutes(KAFKA_MAIN_CONSUMER_POLL_DURATION_MUNITES))
        records.forEach{ fileToProcess =>
          println(fileToProcess.value())
            manageCurrentFile(fileToProcess.value())
        }

        manageFilesToProcess(kafkaConsumer)
    }

    private[this] def startReadinMessages(fileName: String, consumer: KafkaConsumer[String, String], previousMessages: List[String]): Unit = {
        println("INFO - Reading received messages")
        if ((previousMessages != Nil) && previousMessages.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR) {
            println("INFO - save files about to run")
            saveFileBatch(fileName, previousMessages)
            println("SUCCESS - Batch successfully saved - start new batch about to run")
            startReadinMessages(fileName, consumer, Nil)
        } else {

            val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMinutes(KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES))
            val recordsIterator: Iterator[ConsumerRecord[String, String]] = records.iterator().asScala

            val updatedRecordsList: List[String] = prepareNewRecords(previousMessages, recordsIterator)

            startReadinMessages(fileName, consumer, updatedRecordsList)
        }

    }

    private[this] def addRecordToBatch(batch: List[String], record: ConsumerRecord[String, String]): List[String] = {
        batch match {
            case Nil => record.value() :: Nil
            case _ => batch ::: (record.value() :: Nil)
        }
    }

    @scala.annotation.tailrec
    private[this] def prepareNewRecords(batch: List[String], iterator: Iterator[ConsumerRecord[String, String]]): List[String] = {
        if(iterator.hasNext) {
            val updatedBatch: List[String] = addRecordToBatch(batch, iterator.next())
            prepareNewRecords(updatedBatch, iterator)
        } else {
            batch
        }
    }

    private[this] def manageCurrentFile(fileToManage: String): Unit = {
        for {
          kafkaConsumerForFile <- createConsumerForComingFile(fileToManage)
          _ = kafkaConsumerForFile.subscribe(singletonList(fileToManage))
          _ = startReadinMessages(fileToManage, kafkaConsumerForFile, Nil)
          _ = closeKafkaConsumer(kafkaConsumerForFile, KAFKA_FILES_CONSUMER_CLOSE_DURATION_MUNITES)
      } yield ()
    }

    private[this] def getGroupIdForTopic(topic: String): String = {
        s"${topic.toLowerCase}"
    }
    private[this] def createConsumerForComingFile(topic: String): Try[KafkaConsumer[String, String]] = {

        println(s"INFO - Creating Kafka Consumer for file $topic")
        val kafkaProps: Properties = new Properties()

        kafkaProps.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaProps.put("group.id", getGroupIdForTopic(topic))


        Try(new KafkaConsumer[String, String](kafkaProps))
    }

    private[this] def getFileNameForHdfs(file_name: String): String = {
        s"$file_name.$WRITING_FILE_FORMAT"
    }

    private[this] def getFilePathForHdfs(file_name: String): String = {
        s"$HDFS_TARGET_DIR/${getFileNameForHdfs(file_name)}"
    }

    private[this] def saveFileBatch(fileName: String, fileBatchContent: List[String]): Unit = {
        import spark.implicits._

        val batchDataframe: DataFrame =
            fileBatchContent
              .toDF()
              .repartition(NB_DEFAULT_SPARK_PARTITIONS)

        val path: String = getFilePathForHdfs(fileName)

        batchDataframe.write.mode(Append).format(WRITING_FILE_FORMAT).save(path)
    }

    private[this] def closeKafkaConsumer(kafkaConsumer: KafkaConsumer[String, String], durationMinutes: Int): Unit = {
        kafkaConsumer.close(Duration.ofMinutes(durationMinutes))
    }
}

object HistoricDataSaver extends AppConfig {
    def apply(spark: SparkSession): HistoricDataSaver = new HistoricDataSaver(spark)

    val MAIN_KAFKA_TOPIC: String = conf.getString("historic_data.kafka.kafka_topic")
    val MAIN_KAFKA_KEY: String = conf.getString("historic_data.kafka.kafka_key")
    val KAFKA_BOOTSTRAP_SERVERS: String = conf.getString("historic_data.kafka.bootstrap_server")

    val KAFKA_MAIN_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("historic_data.kafka.consumers.kafka_main_consumers_close_duration_minutes")
    val KAFKA_MAIN_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("historic_data.kafka.consumers.kafka_main_consumers_poll_duration_minutes")
    val KAFKA_MAIN_CONSUMERS_GROUP_ID: String = conf.getString("historic_data.kafka.consumers.kafka_main_consumers_group_id")

    val KAFKA_FILES_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("historic_data.kafka.consumers.kafka_files_consumers_close_duration_minutes")
    val KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("historic_data.kafka.consumers.kafka_files_consumers_poll_duration_minutes")
    val KAFKA_FILE_CONSUMERS_GROUP_ID_PREFIX: String = "-group"

    val BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR: Int = conf.getInt("historic_data.kafka.consumers.spark_writing_batch_size")
    val WRITING_FILE_FORMAT: String = conf.getString("historic_data.hdfs_files.file_format")
    val HDFS_TARGET_DIR: String = conf.getString("historic_data.hdfs_files.target_directory")
    val NB_DEFAULT_SPARK_PARTITIONS: Int = conf.getInt("historic_data.spark.default_partitions")
}

