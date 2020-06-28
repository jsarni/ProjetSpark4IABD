package poc.prestacop.HistoricDataLoader

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import poc.prestacop.AppConfig

object HistoricDataLoaderMain extends AppConfig {

    private val KAFKA_BOOTSTRAP_SERVER: String = conf.getString("prestacop.env.kafka_prop.kafka_bootstrap_server")
    private val KAFKA_PRODUCER_CLOSE_DURATION_MINUTES: Int = conf.getInt("historic_data.producers.kafka_producer_close_duration_minutes")

    def main(args: Array[String]): Unit = {

        val kafkaProperties: Properties = new Properties()

        kafkaProperties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val kafkaHitoricalDataProducer: KafkaProducer[String, String] =
            new KafkaProducer[String, String](kafkaProperties)

        HistoricDataLoader(kafkaHitoricalDataProducer).run()
        kafkaHitoricalDataProducer.close(Duration.ofMinutes(KAFKA_PRODUCER_CLOSE_DURATION_MINUTES))
    }
}