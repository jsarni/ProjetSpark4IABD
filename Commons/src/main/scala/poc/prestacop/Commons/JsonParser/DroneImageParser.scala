package poc.prestacop.Commons.JsonParser

import play.api.libs.json._
import poc.prestacop.Commons.schema.DroneImage
import play.api.libs.functional.syntax._
object DroneImageParser {
  implicit val droneImageReads: Reads[DroneImage] = (
    (JsPath \ "image_id").read[String] and
      (JsPath \ "content").read[String]
    ) (DroneImage.apply _)

  implicit val droneImageWrites: Writes[DroneImage] = (
    (JsPath \ "image_id").write[String] and
      (JsPath \ "content").write[String]
    ) (unlift(DroneImage.unapply))
}
