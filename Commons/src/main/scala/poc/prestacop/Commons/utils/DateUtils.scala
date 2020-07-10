package poc.prestacop.Commons.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.{Failure, Success, Try}

object DateUtils {

    def parseHourAndDate(dateString: String, format: String): Option[Timestamp] ={
        Try{
            val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
            val utilDate: Date = dateFormat.parse(dateString)

            new Timestamp(utilDate.getTime);
        } match {
            case Success(value) =>
                Some(value)
            case Failure(_) =>
                None
        }

    }
}
