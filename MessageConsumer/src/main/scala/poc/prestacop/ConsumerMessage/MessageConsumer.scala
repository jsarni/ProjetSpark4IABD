package poc.prestacop.ConsumerMessage
import java.time.Duration

import poc.prestacop.ConsumerMessage.SendMail._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import poc.prestacop.Commons.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import poc.prestacop.Commons.schema.{DroneImage, DroneStandardMessage, DroneViolationMessage}
import poc.prestacop.Commons.JsonParser.DroneViolationMessageParser._
import poc.prestacop.Commons.JsonParser.DroneStandardMessageParser._
import poc.prestacop.Commons.JsonParser.DroneImageParser._
import org.apache.spark.sql.SaveMode.Append

import scala.jdk.CollectionConverters._
import play.api.libs.json._
import java.io._
import java.sql.Timestamp

import scala.util.Try

class MessageConsumer(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, String]) {
  import MessageConsumer._

  def run(): Unit = {
    startReadingMessages(Nil, Nil)
  }
  @scala.annotation.tailrec
  private[this] def startReadingMessages(previousStandardMessages: List[DroneStandardMessage], previousViolationMessages: List[DroneViolationMessage]): Unit = {

    println("INFO - Reading received messages")
    if ((previousStandardMessages != Nil) && previousStandardMessages.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR) {
        println("INFO - save Standard messages  ")
        saveStandardBatch(previousStandardMessages)
        println("SUCCESS - Standard messages Batch successfully saved - start new batch about to run")
        startReadingMessages(Nil, previousViolationMessages)
    }
    else{
      if ((previousViolationMessages != Nil) && previousViolationMessages.length >= BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR) {

        println("INFO - save violation message")
        saveViolationBatch(previousViolationMessages)
        println("SUCCESS - Violation messages Batch successfully saved - start new batch about to run")
        startReadingMessages(previousStandardMessages, Nil)
      }
      else {
        val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMinutes(KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES))
        val recordsIterator: Iterator[ConsumerRecord[String, String]] = records.iterator().asScala
        val updatedLists: (List[DroneStandardMessage], List[DroneViolationMessage]) = manageMessages(previousStandardMessages, previousViolationMessages, recordsIterator)
        startReadingMessages(updatedLists._1, updatedLists._2)
      }
    }
  }
  private[this] def manageMessages(previousStandardMessages: List[DroneStandardMessage],
                                   previousViolationMessages: List[DroneViolationMessage],
                                   iter: Iterator[ConsumerRecord[String, String]]): (List[DroneStandardMessage], List[DroneViolationMessage]) ={
    if(iter.hasNext) {
      val elem = iter.next()
      println(elem.value())
      elem.key() match {

        case KAFKA_STANDARD_KEY =>
          val standardMessageJson:JsValue = Json.parse(elem.value())
          standardMessageJson.validate[DroneStandardMessage] match {
            case s: JsSuccess[DroneStandardMessage] =>
              val standardMessage: DroneStandardMessage = s.get
              (addStandardRecordToBatch(previousStandardMessages, standardMessage), previousViolationMessages)
            case _: JsError =>
              print("ERROR - Lost standard drone message")
              (previousStandardMessages, previousViolationMessages)
          }

        case KAFKA_IMAGE_KEY =>
          val imageMessageJson:JsValue = Json.parse(elem.value())
          imageMessageJson.validate[DroneImage] match {
            case s: JsSuccess[DroneImage] =>
              val imagedMessage: DroneImage = s.get
              saveImage(imagedMessage)
              (previousStandardMessages, previousViolationMessages)
            case _: JsError =>
              print("ERROR - Unsaved violation Image")
              (previousStandardMessages, previousViolationMessages)
          }

        case KAFKA_VIOLATION_KEY =>
          val violationMessageJson:JsValue=Json.parse(elem.value())
          violationMessageJson.validate[DroneViolationMessage] match {
            case s: JsSuccess[DroneViolationMessage] =>
              val violationMessage: DroneViolationMessage = s.get
              val violationCode:String= violationMessage.violation_code.getOrElse("Unknown")
              val violationTime = violationMessage.sending_date
              val violation_date =
                violationTime match{
                  case Some(x)=>
                    x.toString
                  case None =>
                    "unkown"
                }
              val violationDroneId:String= violationMessage.drone_id.getOrElse("Unknown")
              val violation_text :String="Violation occured on "+ violation_date+" \nSent from : "+violationDroneId
              mailSender(violation_text,violationCode)
              (previousStandardMessages, addViolationRecordToBatch(previousViolationMessages, violationMessage))
            case e: JsError =>
              print("error!")
              (previousStandardMessages, previousViolationMessages)
          }

        case _ =>
          printf("WARNING - Unhandled message key")
          (previousStandardMessages, previousViolationMessages)
      }
    } else {
      (previousStandardMessages, previousViolationMessages)
    }
  }

  private[this] def addStandardRecordToBatch(batch: List[DroneStandardMessage], message: DroneStandardMessage): List[DroneStandardMessage] = {
    batch match {
      case Nil =>
        message :: Nil
      case _ =>
        batch ::: (message :: Nil)
    }
  }

  private[this] def addViolationRecordToBatch(batch: List[DroneViolationMessage], message: DroneViolationMessage): List[DroneViolationMessage] = {
    batch match {
      case Nil =>
        message :: Nil
      case _ =>
        batch ::: (message :: Nil)
    }
  }

  private[this] def getFilePathForViolationImage(imageID: String): String = {
    s"$HDFS_IMAGES_TARGET_DIR/$imageID"
  }

  private[this] def saveImage(image: DroneImage): Try[Unit] = {
    Try{
      val filePath: String = getFilePathForViolationImage(image.image_id)
      val writer: PrintWriter = new PrintWriter(filePath)
      writer.write(image.content)
      writer.close()
    }
  }

  private[this] def saveStandardBatch(fileBatchContent: List[DroneStandardMessage]): Unit = {
    import spark.implicits._

    val batchDataframe: DataFrame =
      fileBatchContent.toDF()
        .repartition(NB_DEFAULT_SPARK_PARTITIONS)

    batchDataframe.write.mode(Append).format(WRITING_STANDARD_FILE_FORMAT).save(filePathForStandardMessage)
  }

  private[this] def saveViolationBatch(fileBatchContent: List[DroneViolationMessage]): Unit = {
    import spark.implicits._

    val batchDataframe: DataFrame =
      fileBatchContent.toDF()
        .repartition(NB_DEFAULT_SPARK_PARTITIONS)

    batchDataframe.write.mode(Append).format(WRITING_VIOLATION_FILE_FORMAT).save(filePathForViolationMessage)
  }


  private[this] val filePathForStandardMessage=s"$HDFS_STANDARD_TARGET_DIR/$TARGET_STANDARD_FILE_NAME"
  private[this] val filePathForViolationMessage=s"$HDFS_VIOLATION_TARGET_DIR/$TARGET_VIOLATION_FILE_NAME"


}

object MessageConsumer extends AppConfig {
  def apply(spark: SparkSession, kafkaConsumer: KafkaConsumer[String, String]): MessageConsumer = new MessageConsumer(spark, kafkaConsumer)

  val KAFKA_VIOLATION_KEY=conf.getString("producer_message.kafka.kafka_violation_message_key")
  val KAFKA_STANDARD_KEY=conf.getString("producer_message.kafka.kafka_standard_message_key")
  val KAFKA_IMAGE_KEY=conf.getString("producer_message.kafka.kafka_violation_image_key")

  val MAIN_KAFKA_TOPIC: String = conf.getString("producer_message.kafka.kafka_topic")
  val KAFKA_BOOTSTRAP_SERVERS: String = conf.getString("consumer_message.kafka.bootstrap_server")

  val KAFKA_MAIN_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_close_duration_minutes")
  val KAFKA_MAIN_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_main_consumers_poll_duration_minutes")
  val KAFKA_MAIN_CONSUMERS_GROUP_ID: String = conf.getString("consumer_message.kafka.consumers.kafka_main_consumers_group_id")

  val KAFKA_FILES_CONSUMER_CLOSE_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_close_duration_minutes")
  val KAFKA_FILES_CONSUMER_POLL_DURATION_MUNITES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_poll_duration_minutes")
  val KAFKA_FILE_CONSUMERS_GROUP_ID_PREFIX: String = "-group"

  val BATCH_SIZE_FOR_FILE_WRITING_WITH_SPAR: Int = conf.getInt("consumer_message.kafka.consumers.spark_writing_batch_size")
  val HDFS_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.target_directory")

  val NB_DEFAULT_SPARK_PARTITIONS: Int = conf.getInt("consumer_message.spark.default_partitions")
  //val TARGET_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.file_name")


  val WRITING_STANDARD_FILE_FORMAT: String = conf.getString("consumer_message.hdfs_files.standard_file_format")
  val WRITING_VIOLATION_FILE_FORMAT: String = conf.getString("consumer_message.hdfs_files.violation_file_format")
  val TARGET_STANDARD_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.standard_file_name")
  val HDFS_STANDARD_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.standard_target_dir")
  val TARGET_VIOLATION_FILE_NAME: String = conf.getString("consumer_message.hdfs_files.violation_file_name")
  val HDFS_VIOLATION_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.violation_target_dir")
  val HDFS_IMAGES_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.image_target_dir")

}
