package poc.prestacop.ConsumerMessage

import poc.prestacop.Commons.AppConfig
import org.apache.commons.mail.DefaultAuthenticator
import org.apache.commons.mail.SimpleEmail

object SendMail extends AppConfig {

  private val MAIL_HOST = "smtp.gmail.com"
  private val MAIL_PORT = 465

  private val MAIL_ADRESSE = conf.getString("consumer_message.mail.mail_adress")
//  private val MAIL_USERNAME = conf.getString("consumer_message.mail.user_name")
  private val MAIL_USERNAME = "prestacop.grp10.4iabd1@gmail.com"
  private val MAIL_PASSWORD = "Prestacop.nypd1"
  private val MAIL_SEND_TO = conf.getString("consumer_message.mail.mail_adress_send_to")
  def mailSender(text: String, subject: String) = {

    println(MAIL_ADRESSE, MAIL_USERNAME, MAIL_PASSWORD)

    val email = new SimpleEmail()
    email.setHostName(MAIL_HOST)
    email.setSmtpPort(MAIL_PORT)
    email.setAuthenticator(new DefaultAuthenticator(MAIL_USERNAME, MAIL_PASSWORD))
    email.setSSLOnConnect(true)
    email.setFrom(MAIL_ADRESSE)
    email.setSubject(subject)
    email.setMsg(text)
    email.addTo(MAIL_SEND_TO)
    email.send
  }
}

