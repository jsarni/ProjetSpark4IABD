package poc.prestacop.HistoricDataSaver

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import poc.prestacop.Commons.AppConfig

import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.prestacop.Commons.schema.DroneViolationMessage
import org.apache.spark.sql.SaveMode.Append

import scala.jdk.CollectionConverters._

class HistoricDataSaver(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, DroneViolationMessage]) {

    import HistoricDataSaver._

    def run(): Unit = {
        startReadinMessages(Nil)
    }


    private[this] def startReadinMessages(previousMessages: List[DroneViolationMessage]): Unit = {
        println("INFO - Reading received messages")
        if ((previousMessages != Nil) && previousMessages.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR) {
            println("INFO - save files about to run")
            saveFileBatch(previousMessages)
            kafkaConsumer.commitAsync()
            println("SUCCESS - Batch successfully saved - start new batch about to run")
            startReadinMessages(Nil)
        } else {

            val records: ConsumerRecords[String, DroneViolationMessage] = kafkaConsumer.poll(Duration.ofMinutes(KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES))
            val recordsIterator: Iterator[ConsumerRecord[String, DroneViolationMessage]] = records.iterator().asScala

            val updatedRecordsList: List[DroneViolationMessage] = prepareNewRecords(previousMessages, recordsIterator)

            startReadinMessages(updatedRecordsList)
        }

    }

    private[this] def addRecordToBatch(batch: List[DroneViolationMessage], record: ConsumerRecord[String, DroneViolationMessage]): List[DroneViolationMessage] = {
        batch match {
            case Nil =>
                record.value() :: Nil
            case _ =>
                batch ::: (record.value() :: Nil)
        }
    }

    @scala.annotation.tailrec
    private[this] def prepareNewRecords(batch: List[DroneViolationMessage],
                                        iterator: Iterator[ConsumerRecord[String, DroneViolationMessage]]): List[DroneViolationMessage] = {
        if(iterator.hasNext) {
            val updatedBatch: List[DroneViolationMessage] = addRecordToBatch(batch, iterator.next())
            prepareNewRecords(updatedBatch, iterator)
        } else {
            batch
        }
    }

    private[this] val getFileNameForHdfs: String = {
        s"$TARGET_FILE_NAME.$WRITING_FILE_FORMAT"
    }

    private[this] def getFilePathForHdfs(): String = {
        s"$HDFS_TARGET_DIR/$getFileNameForHdfs"
    }

    private[this] def saveFileBatch(fileBatchContent: List[DroneViolationMessage]): Unit = {
        import spark.implicits._

        val batchDataframe: DataFrame =
            fileBatchContent
              .toDF()
              .repartition(NB_DEFAULT_SPARK_PARTITIONS)

        batchDataframe.write.mode(Append).format(WRITING_FILE_FORMAT).save(getFilePathForHdfs)
    }
}

object HistoricDataSaver extends AppConfig {
    def apply(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, DroneViolationMessage]): HistoricDataSaver = new
        HistoricDataSaver(spark, kafkaConsumer)

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



    val TARGET_FILE_NAME: String = conf.getString("historic_data.hdfs_files.file_name")
    val LINE_SPLIT_REGEX: String = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"
}

