package poc.prestacop.DroneMessageProducer

import java.sql.Timestamp

import scala.util.Random

object DataGenerator {

  def latGenerator(): Double ={
    val lat: Int = -73
    lat - Random.nextDouble()
  }

  def optLatGenerator(): Option[Double] ={
    Some(latGenerator())
  }


  def lngGenerator(): Double = {
    val lng: Int  = 40
    lng + Random.nextDouble()
  }

  def optLngGenerator(): Option[Double] = {
    Some(lngGenerator())
  }

  def dateGenerator(): Timestamp = {
    new Timestamp(System.currentTimeMillis())
  }

  def optDateGenerator(): Option[Timestamp] = {
    Some( dateGenerator())
  }

  def droneIDGenerator(): String = {
    val start: Int = 1
    val end: Int   = 9999999
    String.valueOf(start + Random.nextInt(end - start ))
  }

  def optDroneIDGenerator(): Option[String] = {
    Some(droneIDGenerator())
  }

  def imageIDGenerator(): String = {
    val start: Int = 1
    val end: Int   = 999999999
    String.valueOf(start + Random.nextInt(end - start))
  }

  def optViolationCode(): Option[String] = {
    val start: Int = 1
    val end: Int   = 500
    Some(String.valueOf(start + Random.nextInt(end - start )))
  }

}
