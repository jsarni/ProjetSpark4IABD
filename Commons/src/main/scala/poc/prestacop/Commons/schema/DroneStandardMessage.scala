package poc.prestacop.Commons.schema

import java.sql.Timestamp

case class DroneStandardMessage (
                                  lat: Double,
                                  lng: Double,
                                  sending_date: Timestamp,
                                  drone_id: String
                                )