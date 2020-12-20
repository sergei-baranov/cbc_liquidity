package com.sergei_baranov.cbonds_calc_liquidity.cbc_liquidity_calc

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat

//import com.typesafe.scalalogging.LazyLogging
//import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._

// https://sparkbyexamples.com/spark/spark-dataframe-cache-and-persist-explained/

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

object CalcApp extends App {// with LazyLogging {
  val (
    anchor_date, cbonds_db_path, cbonds_db_login, cbonds_db_password
  ) = (
    args(0), args(1), args(2), args(3)
  )

  // configure log4j
  /*
  val log4jProps = new Properties()
  log4jProps.setProperty("log4j.rootLogger", "WARN, stdout")
  log4jProps.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender")
  log4jProps.setProperty("log4j.appender.stdout.target", "System.out")
  log4jProps.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout")
  log4jProps.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n")
  PropertyConfigurator.configure(log4jProps)
  */

  // prepare db
  println ("DBG: url: [" + "jdbc:mysql://" + cbonds_db_path + "/liquidity?serverTimezone=Europe/Moscow" + "]")
  println ("DBG: anchor_date: [" + anchor_date + "]")
  println ("DBG: cbonds_db_login: [" + cbonds_db_login + "]")

  // make spark jobs

  val appName: String = "My Otus Eurobonds Liquidity"
  var todayDate = "xxx"
  if (0 == anchor_date || "0" == anchor_date || null == anchor_date) {
    val nowHour = CalcAnchorDate.calcHour ()
    println ("DBG: nowHour: [" + nowHour + "]")
    todayDate = CalcAnchorDate.calcDate (nowHour)
  } else {
    todayDate = CalcAnchorDate.customDate (anchor_date)
  }
  println ("DBG: anchorDate: [" + todayDate + "]")

  val (exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath, outputFolder, outputFolderCsv) = (
  "/shara/sergei_baranov_" + todayDate + "/quotes_" + todayDate + ".csv",
  "/shara/sergei_baranov_" + todayDate + "/quotes_mp_month_raw_" + todayDate + ".csv",
  "/shara/sergei_baranov_" + todayDate + "/bonds_" + todayDate + ".csv",
  "/shara/liquidity/parquet_" + todayDate + "/",
  "/shara/liquidity/csv_" + todayDate + "/"
  )

  DbHelper.prepareConnect(cbonds_db_path, cbonds_db_login, cbonds_db_password)
  DbHelper.prepareTables(todayDate)

  /*
  "rm -rf " + outputFolder !!

  "rm -rf " + outputFolderCsv !!
  */

  val conf = new SparkConf ()
  .setMaster ("local[2]")
  .setAppName (appName)

  Runner.run (
    conf, exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath,
    outputFolder, outputFolderCsv,
    cbonds_db_path, cbonds_db_login, cbonds_db_password,
    todayDate)
}

object DbHelper extends App {// with LazyLogging {
  var db_driver: String = ""
  var db_schema: String = ""
  var db_path: String = ""
  var db_url: String = ""
  var db_login: String = ""
  var db_pwd: String = ""
  var mysqlProps: java.util.Properties = null

  def getWeights(): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: getWeights: tsStart: " + tsStart + "; delta " + tsDelta)

    val dfWeights = spark
      .read
      .jdbc(this.db_url, "weights", this.mysqlProps)
      .select(
        $"rank".cast(StringType),
        $"weight".cast(DoubleType)
      )

    //println("DBG: getWeights: dfWeights")
    //dfWeights.show(20)

    val dfSchema = new StructType()
      .add(StructField("rank", StringType, false))
      .add(StructField("weight", DoubleType, false))
    val dfRes = spark.createDataFrame(dfWeights.rdd, dfSchema)
    dfWeights.unpersist()

    //println("DBG: getWeights: dfRes:")
    //dfRes.show(20)

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsStart).toDouble/1000)
    println("DBG: getWeights: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)

    dfRes
  }

  /**
   * set object properties
   * @param bonds_db_path
   * @param cbonds_db_login
   * @param cbonds_db_password
   */
  def prepareConnect(cbonds_db_path: String,
                     cbonds_db_login: String,
                     cbonds_db_password: String): Unit = {
    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: prepareConnect: tsStart: " + tsStart + "; delta " + tsDelta)

    this.db_driver = "com.mysql.cj.jdbc.Driver"
    this.db_schema = "liquidity"
    this.db_path = cbonds_db_path
    var db_url_base = "jdbc:mysql://" + this.db_path + "/" + this.db_schema
    this.db_url = db_url_base + "?serverTimezone=Europe/Moscow" + "&rewriteBatchedStatements=true"
    this.db_login = cbonds_db_login
    this.db_pwd = cbonds_db_password
    var props = new java.util.Properties
    props.setProperty("driver", this.db_driver)
    props.setProperty("user", this.db_login)
    props.setProperty("password", this.db_pwd)
    this.mysqlProps = props

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsStart).toDouble/1000)
    println("DBG: prepareConnect: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)
  }

  /**
   * prepare mysql tables if not exists
   * @param bonds_db_path
   * @param cbonds_db_login
   * @param cbonds_db_password
   */
  def prepareTables(anchor_date: String): Unit = {
    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: prepareTables: tsStart: " + tsStart + "; delta " + tsDelta)

    var connection:Connection = null
    try {
      // make the connection
      Class.forName(this.db_driver)
      connection = DriverManager.getConnection(this.db_url, this.db_login, this.db_pwd)

      // create the statement, and run the query
      val statement = connection.createStatement()
      //statement.executeUpdate("CREATE DATABASE IF NOT EXISTS ${this.db_schema};")
      /*
      // need no more; uncomment if necessary
      statement.executeUpdate("DROP TABLE IF EXISTS " + this.db_schema + ".tsq_metrics;")
       */
      statement.executeUpdate("""
        CREATE TABLE IF NOT EXISTS """ + this.db_schema + """.tsq_metrics (
          id BIGINT NOT NULL DEFAULT 0,
          isin VARCHAR(64) NOT NULL DEFAULT "",
          bid_ask_spread_relative_median DOUBLE NOT NULL DEFAULT 0,
          providers_cnt MEDIUMINT NOT NULL DEFAULT 0,
          qdays_cnt MEDIUMINT NOT NULL DEFAULT 0,
          upsert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id)
        )
        ENGINE = InnoDB;
      """)
      /*
      // need no more; uncomment if necessary
      statement.executeUpdate("DROP TABLE IF EXISTS " + this.db_schema + ".otc_metrics;")
       */
      statement.executeUpdate("""
        CREATE TABLE IF NOT EXISTS """ + this.db_schema + """.otc_metrics (
          id BIGINT NOT NULL DEFAULT 0,
          isin VARCHAR(64) NOT NULL DEFAULT "",
          bid_ask_spread_relative_median DOUBLE NOT NULL DEFAULT 0,
          providers_cnt MEDIUMINT NOT NULL DEFAULT 0,
          qdays_cnt MEDIUMINT NOT NULL DEFAULT 0,
          upsert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id)
        )
        ENGINE = InnoDB;
      """)
      /*
      // need no more; uncomment if necessary
      statement.executeUpdate("DROP TABLE IF EXISTS " + this.db_schema + ".metrics_merged_with_ranks;")
       */
      statement.executeUpdate("""
        CREATE TABLE IF NOT EXISTS """ + this.db_schema + """.metrics_merged_with_ranks (
          id BIGINT NOT NULL DEFAULT 0,
          isin VARCHAR(64) NOT NULL DEFAULT "",
          date_of_end_placing DATE DEFAULT NULL,
          maturity_date DATE DEFAULT NULL,
          anchor_date DATE DEFAULT '1970-01-01',
          days_since_issue MEDIUMINT DEFAULT NULL,
          days_to_maturity MEDIUMINT DEFAULT NULL,
          total_bonds MEDIUMINT DEFAULT NULL,
          tsq_baspread_rel_median DOUBLE DEFAULT NULL,
          otc_baspread_rel_median DOUBLE DEFAULT NULL,
          baspread_rel_median DOUBLE DEFAULT NULL,
          pos_by_baspread_rel_median MEDIUMINT DEFAULT NULL,
          baspread_rel_rank DOUBLE DEFAULT NULL,
          baspread_rel_rank_w DOUBLE DEFAULT NULL,
          tsq_providers_cnt MEDIUMINT DEFAULT NULL,
          otc_providers_cnt MEDIUMINT DEFAULT NULL,
          max_providers_cnt MEDIUMINT DEFAULT NULL,
          providers_cnt MEDIUMINT DEFAULT NULL,
          providers_cnt_rank DOUBLE DEFAULT NULL,
          providers_cnt_rank_w DOUBLE DEFAULT NULL,
          usd_volume DOUBLE DEFAULT NULL,
          pos_by_usd_volume MEDIUMINT DEFAULT NULL,
          usd_volume_rank DOUBLE DEFAULT NULL,
          usd_volume_rank_w DOUBLE DEFAULT NULL,
          tsq_qdays_cnt MEDIUMINT DEFAULT NULL,
          otc_qdays_cnt MEDIUMINT DEFAULT NULL,
          max_qdays_cnt MEDIUMINT DEFAULT NULL,
          qdays_cnt MEDIUMINT DEFAULT NULL,
          qdays_cnt_rank DOUBLE DEFAULT NULL,
          qdays_cnt_rank_w DOUBLE DEFAULT NULL,
          exchange_indicator_rank DOUBLE DEFAULT NULL,
          exchange_indicator_rank_w DOUBLE DEFAULT NULL,
          liq_index DOUBLE DEFAULT NULL,
          upsert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`,`anchor_date`),
          KEY `anchor_date` (`anchor_date`,`id`)
        )
        ENGINE = InnoDB;
      """)
      var delS = "DELETE FROM " + this.db_schema + ".metrics_merged_with_ranks WHERE anchor_date = "+ anchor_date +";"
      println("DBG: delS: " + delS)
      val affectedRows = statement.executeUpdate(delS)
      println("DBG: affectedRows: " + affectedRows)
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }
    try {
      connection.close()
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsStart).toDouble/1000)
    println("DBG: prepareTables: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)
  }

  def flushMergedDf2Table(dfData: DataFrame, tblName: String): Unit = {
    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: flushMergedDf2Table ("+ tblName +"): tsStart: " + tsStart + "; delta " + tsDelta)

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //println("DBG: flushMergedDf2Table: dfData")
    //dfData.show(500)

    println("DBG: flushMergedDf2Table: going to select from dfData 2 arrayData")
    val arrayData = dfData
      .select(
        $"id".cast(LongType),
        $"isin".cast(StringType),

        $"date_of_end_placing".cast(DateType),
        $"maturity_date".cast(DateType),
        $"anchor_date".cast(DateType),

        $"days_since_issue".cast(LongType),
        $"days_to_maturity".cast(LongType),
        $"total_bonds".cast(LongType),

        $"tsq_baspread_rel_median".cast(DoubleType),
        $"otc_baspread_rel_median".cast(DoubleType),
        $"baspread_rel_median".cast(DoubleType),

        $"pos_by_baspread_rel_median".cast(LongType),
        $"baspread_rel_rank".cast(DoubleType),
        $"baspread_rel_rank_w".cast(DoubleType),

        $"tsq_providers_cnt".cast(LongType),
        $"otc_providers_cnt".cast(LongType),
        $"max_providers_cnt".cast(LongType),

        $"providers_cnt".cast(LongType),
        $"providers_cnt_rank".cast(DoubleType),
        $"providers_cnt_rank_w".cast(DoubleType),

        $"usd_volume".cast(DoubleType),
        $"pos_by_usd_volume".cast(LongType),
        $"usd_volume_rank".cast(DoubleType),

        $"usd_volume_rank_w".cast(DoubleType),
        $"tsq_qdays_cnt".cast(LongType),
        $"otc_qdays_cnt".cast(LongType),

        $"max_qdays_cnt".cast(LongType),
        $"qdays_cnt".cast(LongType),
        $"qdays_cnt_rank".cast(DoubleType),

        $"qdays_cnt_rank_w".cast(DoubleType),
        $"exchange_indicator_rank".cast(DoubleType),
        $"exchange_indicator_rank_w".cast(DoubleType),

        $"liq_index".cast(DoubleType)
      )
      .coalesce(1)
      .collect()

    Cal = Calendar.getInstance()
    val tsAfterCollect = Cal.getTime().getTime
    tsDelta = ((tsAfterCollect - tsStart).toDouble/1000)
    println("DBG: flushMergedDf2Table: tsAfterCoalesceAndCollect: " + tsAfterCollect + "; delta " + tsDelta)

    // build query
    var st = """INSERT IGNORE INTO """ + this.db_schema + """.""" + tblName + """
      (id, isin,
      date_of_end_placing, maturity_date, anchor_date,
      days_since_issue, days_to_maturity, total_bonds,
      tsq_baspread_rel_median, otc_baspread_rel_median, baspread_rel_median,
      pos_by_baspread_rel_median, baspread_rel_rank, baspread_rel_rank_w,
      tsq_providers_cnt, otc_providers_cnt, max_providers_cnt,
      providers_cnt, providers_cnt_rank, providers_cnt_rank_w,
      usd_volume, pos_by_usd_volume, usd_volume_rank,
      usd_volume_rank_w, tsq_qdays_cnt, otc_qdays_cnt,
      max_qdays_cnt, qdays_cnt, qdays_cnt_rank,
      qdays_cnt_rank_w, exchange_indicator_rank, exchange_indicator_rank_w,
      liq_index
      )
      VALUES
    """
    var nextValues = ""
    arrayData.foreach { row =>
      nextValues = "\n(\n'" + row.mkString("', '") + "'\n),"
      nextValues = nextValues.replaceAll("'null'", "NULL")
      st = st + nextValues;
    }
    st = st.stripSuffix(",")
    st = st + "\n;"

    //println("DBG: flushMetricsDf2Table: sql: " + st)

    Cal = Calendar.getInstance()
    val tsAfterBuildSql = Cal.getTime().getTime
    tsDelta = ((tsAfterBuildSql - tsAfterCollect).toDouble/1000)
    println("DBG: flushMergedDf2Table: tsAfterBuildSql: " + tsAfterBuildSql + "; delta " + tsDelta)

    var connection:Connection = null
    try {
      // make the connection
      Class.forName(this.db_driver)
      connection = DriverManager.getConnection(this.db_url, this.db_login, this.db_pwd)

      // create the statement
      val statement = connection.createStatement()

      // clear table
      /*
      statement.executeUpdate("""
        TRUNCATE TABLE """ + this.db_schema + """.""" + tblName + """;
      """)
      */
      // and run the query
      val affectedRows = statement.executeUpdate(st)
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }
    try {
      connection.close()
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsAfterBuildSql).toDouble/1000)
    println("DBG: flushMergedDf2Table: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)
  }

  def flushMetricsDf2Table(dfData: DataFrame, tblName: String): Unit = {
    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: flushMetricsDf2Table ("+ tblName +"): tsStart: " + tsStart + "; delta " + tsDelta)

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val arrayData = dfData
      .select(
        $"id".cast(LongType),
        $"isin".cast(StringType),
        $"bid_ask_spread_relative_median".cast(DoubleType),
        $"providers_cnt".cast(LongType),
        $"qdays_cnt".cast(LongType)
      )
      .coalesce(1)
      .collect()

    Cal = Calendar.getInstance()
    val tsAfterCollect = Cal.getTime().getTime
    tsDelta = ((tsAfterCollect - tsStart).toDouble/1000)
    println("DBG: flushMetricsDf2Table: tsAfterCoalesceAndCollect: " + tsAfterCollect + "; delta " + tsDelta)

    // build query
    var st = """INSERT IGNORE INTO """ + this.db_schema + """.""" + tblName + """
      (id, isin, bid_ask_spread_relative_median, providers_cnt, qdays_cnt) VALUES
    """
    var rwId: Long = 0
    var rwIsin: String = ""
    var rwMd: Double = 0.0
    var rwProvs: Long = 0
    var rwDays: Long = 0
    arrayData.foreach { row =>
      //var nextSeq = row.toSeq //.foreach{col => println(col) }
      rwId = row.getLong(0)
      rwIsin = row.getString(1)
      rwMd = row.getDouble(2)
      rwProvs = row.getLong(3)
      rwDays = row.getLong(4)
      st = st + "\n('"+ rwId +"', '"+ rwIsin +"', '"+ rwMd +"', '"+ rwProvs +"','"+ rwDays +"'),"
    }
    st = st.stripSuffix(",")
    st = st + "\n;"
    //println("DBG: flushMetricsDf2Table: sql: " + st)

    Cal = Calendar.getInstance()
    val tsAfterBuildSql = Cal.getTime().getTime
    tsDelta = ((tsAfterBuildSql - tsAfterCollect).toDouble/1000)
    println("DBG: flushMetricsDf2Table: tsAfterBuildSql: " + tsAfterBuildSql + "; delta " + tsDelta)

    var connection:Connection = null
    try {
      // make the connection
      Class.forName(this.db_driver)
      connection = DriverManager.getConnection(this.db_url, this.db_login, this.db_pwd)

      // create the statement
      val statement = connection.createStatement()

      // clear table
      statement.executeUpdate("""
        TRUNCATE TABLE """ + this.db_schema + """.""" + tblName + """;
      """)
      // and run the query
      val affectedRows = statement.executeUpdate(st)
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }
    try {
      connection.close()
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsAfterBuildSql).toDouble/1000)
    println("DBG: flushMetricsDf2Table: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)
  }

  /**
   *
   * @param dfData
   * @param tblName
   */
  def flushDf2Table(dfData: DataFrame, tblName: String): Unit = {
    var tsDelta: Double = 0.0
    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: flushDf2Table ("+ tblName +"): tsStart: " + tsStart + "; delta " + tsDelta)

    // write to mysql

    var connection:Connection = null
    try {
      // make the connection
      Class.forName(this.db_driver)
      connection = DriverManager.getConnection(this.db_url, this.db_login, this.db_pwd)

      dfData
        /*
        // вот такая ерунда у меня на домашней машине (убунта + мариядб),
        // timestamp почему-то в 32 бита, пока что ставлю заплату в коде
        .withColumn(
          "maturity_date",
          when(
            (
              col("maturity_date") > "2037-12-30 00:00:00"
              ),
            "1970-01-01 04:00:01")
            .otherwise(col("maturity_date"))
        )
        .withColumn(
          "date_of_end_placing",
          when(
            (
              col("date_of_end_placing") > "2037-12-30 00:00:00"
              ),
            "1970-01-01 04:00:01")
            .otherwise(col("date_of_end_placing"))
        )
        */
        .coalesce(1)
        .write
        .mode("append")
        .jdbc(this.db_url, tblName, this.mysqlProps)
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }
    try {
      connection.close()
    } catch {
      case e : Throwable => println("ERROR: " + e.getStackTrace.mkString("\nERROR: "))
    }

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsStart).toDouble/1000)
    println("DBG: flushDf2Table ("+ tblName +"): tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)
  }
}

object Runner extends App { //with LazyLogging {
  def run(conf: SparkConf,
          exchangeQuotesFilePath: String,
          otcQuotesFilePath: String,
          eurobondsFilePath: String,
          outputFolder: String,
          outputFolderCsv: String,
          cbonds_db_path: String,
          cbonds_db_login: String,
          cbonds_db_password: String,
          anchorDate: String
         ): Unit = {

    var Cal = Calendar.getInstance()
    var tsDeltaStep = 0.0
    var tsDeltaFunc = 0.0
    val tsStart = Cal.getTime().getTime
    println("DBG: Runner::run(): tsStart: " + tsStart + "; delta 0")

    val spark: SparkSession = {
      SparkSession.builder()
        .config(conf)
        .master("local[2]")
        .getOrCreate()
    }
    spark.sparkContext.setLogLevel("WARN")

    val exchangeQuotes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(exchangeQuotesFilePath)
      .persist(MEMORY_ONLY)

    val otcQuotes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(otcQuotesFilePath)
      .persist(MEMORY_ONLY)

    val bondsList = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(eurobondsFilePath)
      .persist(MEMORY_ONLY)

    Cal = Calendar.getInstance()
    val tsAfterReadCsvs = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterReadCsvs - tsStart).toDouble/1000)
    tsDeltaFunc = ((tsAfterReadCsvs - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterReadCsvs:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    val dfBonds = LiquidityJobber.bondsToStrictDf(bondsList)
      .persist(MEMORY_ONLY)
    bondsList.unpersist()
    // val dfBondsBroadcast = broadcast(dfBonds)

    Cal = Calendar.getInstance()
    val tsAfterBondsToStrict = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterBondsToStrict - tsAfterReadCsvs).toDouble/1000)
    tsDeltaFunc = ((tsAfterBondsToStrict - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterBondsToStrict:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    val tsqMetrics: DataFrame = LiquidityJobber.getTsqMetrics(exchangeQuotes, dfBonds)
      .persist(MEMORY_ONLY)
    exchangeQuotes.unpersist()
    //DbHelper.flushDf2Table(tsqMetrics, "tsq_metrics")
    DbHelper.flushMetricsDf2Table(tsqMetrics, "tsq_metrics")

    Cal = Calendar.getInstance()
    val tsAfterTsqMetrics = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterTsqMetrics - tsAfterBondsToStrict).toDouble/1000)
    tsDeltaFunc = ((tsAfterTsqMetrics - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterTsqMetrics:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    val otcMetrics: DataFrame = LiquidityJobber.getOtcMetrics(otcQuotes, dfBonds)
      .persist(MEMORY_ONLY)
    otcQuotes.unpersist()
    //DbHelper.flushDf2Table(otcMetrics, "otc_metrics")
    DbHelper.flushMetricsDf2Table(tsqMetrics, "otc_metrics")

    Cal = Calendar.getInstance()
    val tsAfterOtcMetrics = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterOtcMetrics - tsAfterTsqMetrics).toDouble/1000)
    tsDeltaFunc = ((tsAfterOtcMetrics - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterOtcMetrics:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    val dfWeights = DbHelper.getWeights().persist(MEMORY_ONLY)
    val mergedMetrics: DataFrame = LiquidityJobber.mergeQuotesMetrics(
      tsqMetrics, otcMetrics, dfBonds, dfWeights, anchorDate)
      .persist(MEMORY_ONLY)
    dfWeights.unpersist()
    DbHelper.flushMergedDf2Table(mergedMetrics, "metrics_merged_with_ranks")

    Cal = Calendar.getInstance()
    val tsAfterMergeAndCalc = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterMergeAndCalc - tsAfterOtcMetrics).toDouble/1000)
    tsDeltaFunc = ((tsAfterMergeAndCalc - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterMergeAndCalc:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    //
    //val bondsLiquidityMetrics: DataFrame = LiquidityJobber.getLiquidityMetrics(tsqMetrics, otcMetrics, bondsList)

    println("DBG: before dfBonds unpersist")
    try {
      dfBonds.unpersist()
    } catch {
      case e: Throwable => println("WARN: dfBonds.unpersist err")
    }
    dfBonds.sparkSession.stop()
    println("DBG: after dfBonds unpersist")

    println("DBG: before tsqMetrics unpersist")
    try {
      tsqMetrics.unpersist()
    } catch {
      case e: Throwable => println("WARN: tsqMetrics.unpersist err")
    }
    tsqMetrics.sparkSession.stop()
    println("DBG: after tsqMetrics unpersist")

    println("DBG: before otcMetrics unpersist")
    try {
      otcMetrics.unpersist()
    } catch {
      case e: Throwable => println("WARN: otcMetrics.unpersist err")
    }
    otcMetrics.sparkSession.stop()
    println("DBG: after otcMetrics unpersist")

    println("DBG: before mergedMetrics unpersist")
    try {
      mergedMetrics.unpersist()
    } catch {
      case e: Throwable => println("WARN: mergedMetrics.unpersist err")
    }
    mergedMetrics.sparkSession.stop()
    println("DBG: after mergedMetrics unpersist")

    println("DBG: before spark stop")
    spark.stop()
    println("DBG: after spark stop")

    Cal = Calendar.getInstance()
    val tsAfterSparkStop = Cal.getTime().getTime
    tsDeltaStep = ((tsAfterSparkStop - tsAfterMergeAndCalc).toDouble/1000)
    tsDeltaFunc = ((tsAfterSparkStop - tsStart).toDouble/1000)
    println("DBG: Runner::run(): tsAfterSparkStop:  deltaStep " + tsDeltaStep
      + ", deltaFunc " + tsDeltaFunc)

    sys.exit()
  }
}
