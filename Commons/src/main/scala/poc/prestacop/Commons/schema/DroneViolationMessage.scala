package poc.prestacop.Commons.schema

import java.sql.Timestamp

case class DroneViolationMessage(
                                  lat: Option[Double],
                                  lng: Option[Double],
                                  sending_date: Option[Timestamp],
                                  drone_id: Option[String],
                                  violation_code: Option[String],
                                  image_id: Option[String]
                               ) extends DroneMessage
