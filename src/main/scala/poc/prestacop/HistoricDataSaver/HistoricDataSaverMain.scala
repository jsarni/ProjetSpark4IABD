package poc.prestacop.HistoricDataSaver

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
object HistoricDataSaverMain {

    def main(args: Array[String]): Unit = {

        val kafkaProps: Properties = new Properties()

        kafkaProps.put("bootstrap.servers", "localhost:9092")
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        val kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](kafkaProps)

        HistoricDataSaver(kafkaConsumer).run()
    }
}
