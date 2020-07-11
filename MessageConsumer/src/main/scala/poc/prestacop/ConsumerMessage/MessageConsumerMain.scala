package poc.prestacop.ConsumerMessage

import java.time.Duration
import java.util.Collections.singletonList
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession
import poc.prestacop.Commons.AppConfig

object MessageConsumerMain extends AppConfig{

  private val SPARKSESSION_APPNAME: String = conf.getString("consumer_message.spark.appname")
  private val SPARKSESSION_MASTER: String = conf.getString("consumer_message.spark.master")
  private val KAFKA_BOOTSTRAP_SERVER: String = conf.getString("prestacop.env.kafka_prop.kafka_bootstrap_server")
  private val KAFKA_CONSUMERS_GROUP_ID: String = conf.getString("consumer_message.kafka.consumers.kafka_main_consumers_group_id")
  private val KAFKA_TOPIC: String = conf.getString("producer_message.kafka.kafka_topic")
  private val KAFKA_CONSUMER_CLOSE_DURATION_MINUTES: Int = conf.getInt("consumer_message.kafka.consumers.kafka_files_consumers_close_duration_minutes")


  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession =
      SparkSession
        .builder()
        .master(SPARKSESSION_MASTER)
        .appName(SPARKSESSION_APPNAME)
        .getOrCreate()

    val kafkaProperties: Properties = new Properties()

    kafkaProperties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("group.id", KAFKA_CONSUMERS_GROUP_ID)


    val kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProperties)

    kafkaConsumer.subscribe(singletonList(KAFKA_TOPIC))

    MessageConsumer(sparkSession, kafkaConsumer).run()

    kafkaConsumer.close(Duration.ofMinutes(KAFKA_CONSUMER_CLOSE_DURATION_MINUTES))
    sparkSession.stop()
  }
}

