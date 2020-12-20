package com.sergei_baranov.cbonds_calc_liquidity.cbc_liquidity_datain

import sys.process._
import java.io.File
import java.net.{Authenticator, PasswordAuthentication, URL}

//import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils

// далее по коду Authenticator.setDefault принимает объект,
// который видимо должен вернуть креденшлы в методе getPasswordAuthentication
class MyAuthenticator(val username: String, val password: String) extends Authenticator {
  var authUserName: String = username;
  var authPassword: String = password;

  override protected def getPasswordAuthentication = new PasswordAuthentication(this.authUserName, this.authPassword.toCharArray)

  def getAuthUserName: String = this.authUserName

  def getAuthPassword: String = this.authPassword
}

/**
 * Забирает и складывает в файловой системе исходные данные для
 * расчётов метрик ликвидности (zip с 4-мя csv-шками)
 * Файлы потом используются как исходныен данные в AnalyticsConsumer
 *
 * @param MyCurrentAuthenticator
 * @param todayDate
 */
class Stage4Batch(MyCurrentAuthenticator: MyAuthenticator, todayDate: String,
                  cbdb_client_dir: String) extends App { //with StrictLogging {
  def mkJob(): Unit = {
    // shara - я прописал такой volume во всех докерах
    "touch /shara/somefile.txt" !!

    // Выкачиваем zip со списком евробондов россии и снг и архивами котировок (биржевые и внебиржевые).
    // соглашение по именованию на источнике: username_yyyMMdd.zip
    // FileUtils.copyURLToFile всё делает, используя Authenticator
    val zipName = cbdb_client_dir + "_" + todayDate + ".zip"
    val zipPathRemote = "https://database.cbonds.info/unloads/"+ cbdb_client_dir +"/archive/" + zipName
    val tmpDir = "/shara/tmp/"
    val tmpZipPath = tmpDir + zipName
    val tmpUnzippedDir = "/shara/"+ cbdb_client_dir +"_" + todayDate + "/"

    //logger.info("zipPathRemote: [" + zipPathRemote + "]")
    println("zipPathRemote: [" + zipPathRemote + "]")
    //logger.info("tmpZipPath: [" + tmpZipPath + "]")
    println("tmpZipPath: [" + tmpZipPath + "]")
    //logger.info("tmpUnzippedDir: [" + tmpUnzippedDir + "]")
    println("tmpUnzippedDir: [" + tmpUnzippedDir + "]")

    /** @TODO обработка исключений */

    "rm -f " + tmpZipPath !!

    "rm -rf " + tmpUnzippedDir !!

    FileUtils.copyURLToFile(new URL(zipPathRemote), new File(tmpZipPath))
    //logger.info("copyURLToFile:" + "ok")
    println("copyURLToFile:" + "ok")

    "mkdir " + tmpUnzippedDir !!

    "unzip " + tmpZipPath + " -d " + tmpUnzippedDir !!

    val lsDir = "ls -lt " + tmpUnzippedDir !!

    //logger.info(lsDir)
    println(lsDir)

    "rm -f " + tmpZipPath !!

    "rm -rf " + tmpDir !!

    val lsDir2 = "ls -alt /shara" !!

    //logger.info(lsDir2)
    println(lsDir2)
  }
}
