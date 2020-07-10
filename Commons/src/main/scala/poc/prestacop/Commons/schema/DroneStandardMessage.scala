package poc.prestacop.Commons.schema

import java.sql.Date

case class DroneStandardMessage(
                                 lat: Double,
                                 lng: Double,
                                 sendingDate: Date,
                                 drone_id: String
                               )
