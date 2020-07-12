package poc.prestacop.DroneMessageProducer
import java.sql.Timestamp
import play.api.libs.json._
import poc.prestacop.DroneMessageProducer.DataGenerator._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import poc.prestacop.Commons.AppConfig
import poc.prestacop.Commons.schema.{DroneImage, DroneStandardMessage, DroneViolationMessage}
import poc.prestacop.Commons.JsonParser.DroneImageParser._
import poc.prestacop.Commons.JsonParser.DroneStandardMessageParser._
import poc.prestacop.Commons.JsonParser.DroneViolationMessageParser._
import scala.util.Random

class MessageProducer (producer: KafkaProducer[String, String]){
  import MessageProducer._
  
  def run() : Unit ={
    generateRandomDroneMessages()

  }

  private[this] def generateRandomDroneStandardMessage(): DroneStandardMessage ={
    val lat :Double =  latGenerator()
    val lng: Double = lngGenerator()
    val sending_date: Timestamp = dateGenerator()
    val drone_id: String = droneIDGenerator()
    DroneStandardMessage(lat,lng,sending_date,drone_id)
  }

  private[this] def generateRandomImage(): DroneImage ={
    val image_id : String = imageIDGenerator()
    DroneImage(image_id)
  }

  private[this] def generateRandomDroneViolationMessage() : (DroneViolationMessage,DroneImage) = {
    val lat : Option[Double] =  optLatGenerator()
    val lng: Option[Double] = optLngGenerator()
    val sending_date: Option[Timestamp] = optDateGenerator()
    val drone_id: Option[String]  = optDroneIDGenerator()
    val violation_code: Option[String] = optViolationCode()
    val image : DroneImage = generateRandomImage()
    val image_id: Option[String] = Some(image.image_id)
    (DroneViolationMessage(lat,lng,sending_date,drone_id,violation_code,image_id),image)

  }
  @scala.annotation.tailrec
  private[this] def generateRandomDroneMessages(): Unit ={
    val rand: Int = 1 + Random.nextInt(20)
    if (rand == 12) {
      val (message,image): (DroneViolationMessage,DroneImage) = generateRandomDroneViolationMessage()
      val parsedViolationMessage: String  = jsonToString(Json.toJson(message))
      val parseImage: String = jsonToString(Json.toJson(image))
      producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_VIOLATION_MESSAGE_KEY, parsedViolationMessage))
      Thread.sleep(1000)
      producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_VIOLATION_IMAGE_KEY, parseImage))
    }
    else {
      val parsedStandardMessage: String = jsonToString(Json.toJson(generateRandomDroneStandardMessage()))
      producer.send(new ProducerRecord(KAFKA_TOPIC, KAFKA_STANDARD_MESSAGE_KEY, parsedStandardMessage))
    }
    Thread.sleep(1000)
    generateRandomDroneMessages()
  }

  private[this] def jsonToString(content: JsValue): String ={
    String.valueOf(content)
  }
}
object MessageProducer extends AppConfig{
  def apply(producer: KafkaProducer[String, String]): MessageProducer = new MessageProducer(producer)
  private val KAFKA_TOPIC: String = conf.getString("kafka.kafka_topic")
  private val KAFKA_STANDARD_MESSAGE_KEY: String = conf.getString("kafka.kafka_standard_message_key")
  private val KAFKA_VIOLATION_MESSAGE_KEY: String = conf.getString("kafka.kafka_violation_message_key")
  private val KAFKA_VIOLATION_IMAGE_KEY: String = conf.getString("kafka.kafka_violation_image_key")

}