package poc.prestacop.Commons.schema

import java.sql.Date

case class DroneViolationMessage(
                                 lat: Double,
                                 lng: Double,
                                 sending_date: Date,
                                 drone_id: String,
                                 violation_code: String,
                                 image_id: String
                               )
