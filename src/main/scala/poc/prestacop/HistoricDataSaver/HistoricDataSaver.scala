package poc.prestacop.HistoricDataSaver

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import poc.prestacop.AppConfig

import scala.util.Try
import java.util.Collections._

class HistoricDataSaver(kafkaConsumer: KafkaConsumer[String, String]) {

    import HistoricDataSaver._

    def run(): Try[Unit] = {
        getTopicsToManage()
    }
    private[this] def getTopicsToManage(): Try[Unit] = {
        Try{
            kafkaConsumer.subscribe(singletonList(MAIN_KAFKA_TOPIC))

            val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofMinutes(5))

            records.forEach{ record =>
                println(record.value())
            }

        }
    }

}

object HistoricDataSaver extends AppConfig {
    def apply(kafkaConsumer: KafkaConsumer[String, String]): HistoricDataSaver = new HistoricDataSaver(kafkaConsumer)

    val MAIN_KAFKA_TOPIC: String = conf.getString("historic_data.kafka.kafka_topic")
    val MAIN_KAFKA_KEY: String = conf.getString("historic_data.kafka.kafka_key")
}

