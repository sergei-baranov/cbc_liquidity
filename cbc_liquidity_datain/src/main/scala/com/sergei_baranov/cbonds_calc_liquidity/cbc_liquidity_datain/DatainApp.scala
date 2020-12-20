package com.sergei_baranov.cbonds_calc_liquidity.cbc_liquidity_datain

import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat

//import akka.actor.{Props}

//import com.typesafe.scalalogging._
//import com.typesafe.scalalogging.LazyLogging
//import com.typesafe.scalalogging.StrictLogging
//import org.apache.kafka.clients.producer.{KafkaProducer}

import java.net.Authenticator

//import akka.actor.ActorSystem

object CalcAnchorDate {
  def calcHour(): Int = {
    val Cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    Cal.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"))
    val nowHour = Cal.get(Calendar.HOUR_OF_DAY)

    nowHour
  }

  def calcDate(nowHour: Int): String = {
    // если сейчас До 10-ти по Мск - то за todayDate надо считать вчера
    val format = new SimpleDateFormat("yyyMMdd")
    val Cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    Cal.setTimeZone(TimeZone.getTimeZone("GMT"))
    val gmtTime = Cal.getTime.getTime
    var timezoneAlteredTime = gmtTime + TimeZone.getTimeZone("Europe/Moscow").getRawOffset
    if (nowHour < 10) {
      timezoneAlteredTime -= 86400000;
    }
    Cal.setTimeInMillis(timezoneAlteredTime)
    val dtMillis = Cal.getTime()
    val todayDate = (format.format(dtMillis))

    todayDate
  }

  def customDate(anchorDate: String): String = {
    val formatIn = new SimpleDateFormat("yyyy-MM-dd")
    //val dateObj = formatIn.parse("2018-03-03")
    val dateObj = formatIn.parse(anchorDate)
    val format = new SimpleDateFormat("yyyMMdd")
    val todayDate = (format.format(dateObj))

    todayDate
  }
}

object DatainApp extends App {// with StrictLogging {

  // запрашиваем креденшлы для сервисов CBonds
  val (cbonds_username, cbonds_password) = (args(3), args(4)) // see Dockerfile
  // и сохраняем их в объект
  val MyCurrentAuthenticator = new MyAuthenticator(cbonds_username, cbonds_password)
  Authenticator.setDefault(MyCurrentAuthenticator)

  val cbdb_client_dir = args(2)
  println("cbdb_client_dir: [" + cbdb_client_dir + "]")

  // запрашиваем anchor_date
  val (anchor_date)  = (args(1)) // see Dockerfile
  //logger.info("anchor_date: [" + anchor_date + "]")
  println("anchor_date: [" + anchor_date + "]")
  var todayDate = "xxx"
  if (0 == anchor_date || "0" == anchor_date || null == anchor_date) {
    // до 10-ти утра по МСК не надо работать стримы
    // (пока не решили точно,
    // сейчас стримы тоже работаем кроглосуточно,
    // далее по коду if (false &&), if (true ||)),
    // а данные для батчей брать, но на вчера по МСК
    val nowHour = CalcAnchorDate.calcHour()
    //logger.info("nowHour: [" + nowHour + "]")
    println("nowHour: [" + nowHour + "]")
    // соответственно todayDate до 10-тичасов содержит вчерашнюю дату
    todayDate = CalcAnchorDate.calcDate(nowHour)
    //logger.info("anchorDate: [" + todayDate + "]")
    println("anchorDate: [" + todayDate + "]")
  } else {
    todayDate = CalcAnchorDate.customDate(anchor_date)
    //logger.info("anchorDate: [" + todayDate + "]")
    println("anchorDate: [" + todayDate + "]")
  }

  // Сначала скачиваем исходные данные для вычисления метрик ликвидности
  //logger.info("Download data for batch jobber")
  println("Download data for batch jobber")
  val Stager4Batch = new Stage4Batch(MyCurrentAuthenticator, todayDate, cbdb_client_dir)
  Stager4Batch.mkJob()
  //logger.info("Data for batch jobber downloaded")
  println("Data for batch jobber downloaded")
}