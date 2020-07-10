package poc.prestacop.ConsumerMessage
import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import poc.prestacop.Commons.AppConfig

import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.prestacop.Commons.schema.DroneMessage
import org.apache.spark.sql.SaveMode.Append

import scala.jdk.CollectionConverters._
import scala.util.Try
import java.util.Collections._
import java.util.Properties


class ConsumerMessage(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, DroneMessage]) {
  import ConsumerMessage._

  def run(): Unit = {
    startReadingMessages(Nil)
  }
  private[this] def startReadingMessages(previousMessages: List[DroneMessage]): Unit = {
    println("INFO - Reading received messages")

    if ((previousMessages != Nil) && previousMessages.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR) {
      println("INFO - save files about to run")
      saveFileBatch(previousMessages)
      println("SUCCESS - Batch successfully saved - start new batch about to run")
      startReadingMessages(Nil)
    } else {

      val records: ConsumerRecords[String, DroneMessage] = kafkaConsumer.poll(Duration.ofMinutes(KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES))
      val recordsIterator: Iterator[ConsumerRecord[String, DroneMessage]] = records.iterator().asScala
      val updatedRecordsList: List[DroneMessage] = prepareNewRecords(previousMessages, recordsIterator)

      startReadingMessages(updatedRecordsList)
    }

  }

  private[this] def addRecordToBatch(batch: List[DroneMessage], record: ConsumerRecord[String, DroneMessage]): List[DroneMessage] = {
    batch match {
      case Nil =>
        record.value() :: Nil
      case _ =>
        batch ::: (record.value() :: Nil)
    }
  }

  @scala.annotation.tailrec
  private[this] def prepareNewRecords(batch: List[DroneMessage],
                                      iterator: Iterator[ConsumerRecord[String, DroneMessage]]): List[DroneMessage] = {
    if(iterator.hasNext) {
      val updatedBatch: List[DroneMessage] = addRecordToBatch(batch, iterator.next())
      prepareNewRecords(updatedBatch, iterator)
    } else {
      batch
    }
  }

  private[this] val getFileNameForHdfsViolation: String = {
    s"$TARGET_FILE_NAME.$WRITING_FILE_FORMAT"
  }

  private[this] def getFilePathForHdfsViolation(): String = {
    s"$HDFS_TARGET_DIR/$getFileNameForHdfsViolation"
  }
  private[this] val getFileNameForHdfsStandard: String = {
    s"$TARGET_FILE_NAME.$WRITING_FILE_FORMAT"
  }

  private[this] def getFilePathForHdfsStandard(): String = {
    s"$HDFS_TARGET_DIR/$getFileNameForHdfsStandard"
  }
  private[this] def saveFileBatch(fileBatchContent: List[DroneMessage]): Unit = {
    import spark.implicits._

    val batchDataframe: DataFrame =
      fileBatchContent.toDF()
        .repartition(NB_DEFAULT_SPARK_PARTITIONS)

    batchDataframe.write.mode(Append).format(WRITING_FILE_FORMAT).save(getFilePathForHdfsViolation())
  }

  private[this] def standardOrViolation(file:List[DroneMessage]): Unit ={
    val type_file =file.getClass()

  }
}

object ConsumerMessage extends AppConfig {
  def apply(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, DroneMessage]): ConsumerMessage = new
      ConsumerMessage(spark, kafkaConsumer)

  val MAIN_KAFKA_TOPIC: String = conf.getString("consumer_message.kafka.kafka_topic")
  val MAIN_KAFKA_KEY: String = conf.getString("consumer_message.kafka.kafka_key")
  val KAFKA_BOOTSTRAP_SERVERS: String = conf.getString("consumer_message.kafka.bootstrap_server")

  val KAFKA_MAIN_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_close_duration_minutes")
  val KAFKA_MAIN_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_poll_duration_minutes")
  val KAFKA_MAIN_CONSUMERS_GROUP_ID: String = conf.getString("consumer_message.kafka.consumers.kafka_main_consumers_group_id")

  val KAFKA_FILES_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_close_duration_minutes")
  val KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_poll_duration_minutes")
  val KAFKA_FILE_CONSUMERS_GROUP_ID_PREFIX: String = "-group"

  val BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR: Int = conf.getInt("consumer_message.kafka.consumers.spark_writing_batch_size")
  val WRITING_FILE_FORMAT: String = conf.getString("consumer_message.hdfs_files.file_format")
  val HDFS_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.target_directory")
  val NB_DEFAULT_SPARK_PARTITIONS: Int = conf.getInt("consumer_message.spark.default_partitions")



  val TARGET_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.file_name")
  val LINE_SPLIT_REGEX: String = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

}
