package poc.prestacop.Commons.serializer

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import org.apache.kafka.common.serialization.Serializer
import poc.prestacop.Commons.schema.DroneViolationMessage


class DroneViolationMessageSerializer extends Serializer[DroneViolationMessage]{

    override def configure(configs: util.Map[String,_],isKey: Boolean):Unit = {

    }


    override def serialize(topic:String, data: DroneViolationMessage):Array[Byte] = {
        try {
            val byteOut = new ByteArrayOutputStream()
            val objOut = new ObjectOutputStream(byteOut)
            objOut.writeObject(data)
            objOut.close()
            byteOut.close()
            byteOut.toByteArray
        }
        catch {
            case ex:Exception => throw new Exception(ex.getMessage)
        }
    }

    override def close():Unit = {

    }


}
