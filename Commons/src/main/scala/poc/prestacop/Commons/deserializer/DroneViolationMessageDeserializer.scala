package poc.prestacop.Commons.deserializer
import poc.prestacop.Commons.schema.DroneViolationMessage
import java.io.{ObjectInputStream, ByteArrayInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer

class DroneViolationMessageDeserializer extends Deserializer[DroneViolationMessage]{

    override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

    }
    override def deserialize(topic:String,bytes: Array[Byte]): DroneViolationMessage = {
        val byteIn = new ByteArrayInputStream(bytes)
        val objIn = new ObjectInputStream(byteIn)
        val obj = objIn.readObject().asInstanceOf[DroneViolationMessage]
        byteIn.close()
        objIn.close()
        obj
    }
    override def close():Unit = {

    }

}
