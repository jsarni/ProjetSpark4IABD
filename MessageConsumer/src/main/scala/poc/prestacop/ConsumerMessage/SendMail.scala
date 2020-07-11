package poc.prestacop.ConsumerMessage


import java.util.Properties
import javax.mail.{Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import poc.prestacop.Commons.AppConfig
import scala.io.Source


object SendMail extends AppConfig {
  private val MAIL_HOST = "smtp.gmail.com"
  private val MAIL_PORT = "587"

  private val MAIL_ADRESSE = conf.getString("consumer_message.mail.mail_adress")
  private val MAIL_USERNAME = conf.getString("consumer_message.mail.user_name")
  private val MAIL_PASSWORD = conf.getString("consumer_message..mail.password")

  def mailSender(text:String, subject:String) = {
    val properties = new Properties()
    properties.put("mail.smtp.port", MAIL_PORT)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")

    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(MAIL_ADRESSE));
    message.setSubject(subject)
    message.setContent(text, "text/html")

    val transport = session.getTransport("smtp")
    transport.connect(MAIL_HOST, MAIL_USERNAME,MAIL_PASSWORD)
    transport.sendMessage(message, message.getAllRecipients)
    //(Nil,Nil)
  }

}
