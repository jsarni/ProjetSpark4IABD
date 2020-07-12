package poc.prestacop.Commons.JsonParser

import java.sql.Timestamp

import play.api.libs.json._
import play.api.libs.json.Reads._
import poc.prestacop.Commons.schema.DroneViolationMessage
import play.api.libs.functional.syntax._

object DroneViolationMessageParser {

  def timestampToLong(t: Timestamp): Long = t.getTime

  def longToTimestamp(dt: Long): Timestamp = new Timestamp(dt)

  implicit val timestampFormat: Format[Timestamp] = new Format[Timestamp] {
    def writes(t: Timestamp): JsValue = Json.toJson(timestampToLong(t))

    def reads(json: JsValue): JsResult[Timestamp] = Json.fromJson[Long](json).map(longToTimestamp)
  }

  implicit val droneViolationMessageReads: Reads[DroneViolationMessage] = (
    (JsPath \ "lat").readNullable[Double] and
      (JsPath \ "lng").readNullable[Double] and
      (JsPath \ "sending_date").readNullable[Timestamp] and
      (JsPath \ "drone_id").readNullable[String] and
      (JsPath \ "violation_code").readNullable[String] and
      (JsPath \ "image_id").readNullable[String]
    ) (DroneViolationMessage.apply _)
}
