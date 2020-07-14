package poc.prestacop.ConsumerMessage

import poc.prestacop.Commons.AppConfig
import org.apache.commons.mail.DefaultAuthenticator
import org.apache.commons.mail.SimpleEmail
import poc.prestacop.Commons.schema.DroneViolationMessage
import poc.prestacop.Commons.utils.DateUtils.convertTimestampToString

object SendMail extends AppConfig {

  private val MAIL_HOST = "smtp.gmail.com"
  private val MAIL_PORT = 465

  private val MAIL_ADRESSE = conf.getString("consumer_message.mail.mail_adress")
  private val MAIL_USERNAME = conf.getString("consumer_message.mail.user_name")
  private val MAIL_PASSWORD = conf.getString("consumer_message.mail.password")
  private val MAIL_SEND_TO = conf.getString("consumer_message.mail.mail_adress_send_to")
  private val HDFS_IMAGES_TARGET_DIR: String = conf.getString("consumer_message.hdfs_files.image_target_dir")

  def mailSender(violationMessage: DroneViolationMessage) = {

    val messageText: String = convertMessageToMail(violationMessage)
    val subject: String = s"Violation Alert"

    val email = new SimpleEmail()
    email.setHostName(MAIL_HOST)
    email.setSmtpPort(MAIL_PORT)
    email.setAuthenticator(new DefaultAuthenticator(MAIL_USERNAME, MAIL_PASSWORD))
    email.setSSLOnConnect(true)
    email.setFrom(MAIL_ADRESSE)
    email.setSubject(subject)
    email.setMsg(messageText)
    email.addTo(MAIL_SEND_TO)
    email.send
  }

  private[this] def convertMessageToMail(violationMessage: DroneViolationMessage): String = {
    val violationCode: String= violationMessage.violation_code.getOrElse("Unknown")
    val violationDate =
      violationMessage.sending_date match{
        case Some(timestamp)=>
          convertTimestampToString(timestamp)
        case None =>
          "Unknown"
      }
    val violationDroneId: String= violationMessage.drone_id.getOrElse("Unknown")
    val violationLat = violationMessage.lat match{
      case Some(latitude)=>
        String.valueOf(latitude)
      case None =>
        "Unknown"
    }
    val violationLng = violationMessage.lng match{
      case Some(longitude)=>
        String.valueOf(longitude)
      case None =>
        "Unknown"
    }

    val violationImagePath: String = violationMessage.image_id match {
      case Some(imageID)=>
        imagePath(imageID)
      case None =>
        "Unknown"
    }
    s"""|New Violation Occured
        |Violation Code: $violationCode
        |Occurring Date: $violationDate
        |Latitude : $violationLat
        |Longitude : $violationLng
        |Drone ID: $violationDroneId
        |Violation Image: $violationImagePath
        |
        |
        |-- SENT FROM ALERT HANDLER SYSTEM --
        |""".stripMargin
  }

  private[this] def imagePath(imageID: String): String = {
    s"$HDFS_IMAGES_TARGET_DIR/$imageID"
  }
}

