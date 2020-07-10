package poc.prestacop.Commons.JsonParser

import java.sql.Timestamp

import play.api.libs.json._
import play.api.libs.json.Reads._
import poc.prestacop.Commons.schema.DroneStandardMessage
import play.api.libs.functional.syntax._

object DroneStandardMessageParser {
  def timestampToLong(t: Timestamp): Long = t.getTime
  def longToTimestamp(dt: Long): Timestamp = new Timestamp(dt)

  implicit val timestampFormat: Format[Timestamp] = new Format[Timestamp] {
    def writes(t: Timestamp): JsValue = Json.toJson(timestampToLong(t))
    def reads(json: JsValue): JsResult[Timestamp] = Json.fromJson[Long](json).map(longToTimestamp)
  }

  implicit val droneStandarMessageReads: Reads[DroneStandardMessage] = (
    (JsPath \ "lat").read[Double](min(-90.0) keepAnd max(90.0)) and
      (JsPath \ "lng").read[Double](min(-180.0) keepAnd max(180.0)) and
      (JsPath \ "sending_date").read[Timestamp] and
      (JsPath \ "drone_id").read[String]

    )(DroneStandardMessage.apply _)
}
