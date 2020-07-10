package poc.prestacop.Commons.JsonParser

import play.api.libs.json._
import poc.prestacop.Commons.schema.DroneImage
import play.api.libs.functional.syntax._
object DroneImageParser {
  implicit val droneImageReads: Reads[DroneImage] = (
    (JsPath \ "image-id").read[String] and
      (JsPath \ "content").read[String]
    ) (DroneImage.apply _)
}
