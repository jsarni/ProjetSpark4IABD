package poc.prestacop.ConsumerMessage
import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import poc.prestacop.Commons.AppConfig

import scala.util.Try
import java.util.Collections._
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode.Append

import scala.jdk.CollectionConverters._

class ConsumerMessage {

}
