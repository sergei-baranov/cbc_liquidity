package com.sergei_baranov.cbonds_calc_liquidity.cbc_liquidity_calc

import java.util.Calendar

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.pow
import org.apache.spark.sql.functions.exp
import org.apache.spark.sql.functions.log
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._

object LiquidityJobber {
  /*
  val spark: SparkSession = SparkSession.getActiveSession.get
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  */

  def bondsToStrictDf(bondsList: DataFrame): DataFrame = {
    var tsDelta: Double = 0.0

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime

    println("DBG: bondsToStrictDf: tsStart: " + tsStart + "; delta 0")

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val bondsListStrictClean = bondsList
      .filter(
        $"id".isNotNull
        && $"isin_code".isNotNull
      )
      .select(
        $"id".as("id").cast(LongType),
        $"isin_code".as("isin").cast(StringType),
        $"usd_volume".as("usd_volume").cast(DoubleType),
        $"date_of_end_placing".as("date_of_end_placing").cast(DateType),
        $"maturity_date".as("maturity_date").cast(DateType)
      )
    val bondsSchema = new StructType()
      .add(StructField("id",                  LongType, false))
      .add(StructField("isin",                StringType, false))
      .add(StructField("usd_volume",          DoubleType, true))
      .add(StructField("date_of_end_placing", DateType, true))
      .add(StructField("maturity_date",       DateType, true))
    val dfBonds = spark.createDataFrame(bondsListStrictClean.rdd, bondsSchema)

    bondsListStrictClean.unpersist()
    bondsList.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsStart).toDouble/1000)
    println("DBG: bondsToStrictDf: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)

    // return
    dfBonds
  }

  /**
   *
   */
  def getQuotMetrics(quotes: DataFrame, bonds: DataFrame): DataFrame = {
    var tsDelta: Double = 0.0

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime
    println("DBG: getQuotMetrics: tsStart: " + tsStart + "; delta 0")

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 4. bid_ask_spread_relative_median, bid_ask_spread_relative_rank
    /*
    bid_ask_spread_relative_median:
    считаем относительный bid-ask спред по каждой записи,
    на каждую дату считаем медиану bid-ask спреда по биржам для бумаги,
    и далее - медиану ото всех дат для бумаги
    //bid_ask_spread_relative_rank:
    //сортируем бонды desc по bid_ask_spread_relative_median
    //и делим позицию бумаги на общее количество бумаг
    */
    // считаем относительный bid-ask спред по каждой записи
    val dfBASpread = quotes
      .withColumn("ba_spread", (($"ask" - $"bid")/(($"ask" + $"bid")/2)))
      .persist(MEMORY_ONLY)
    // на каждую дату считаем медиану bid-ask спреда по биржам для бумаги
    dfBASpread.createOrReplaceTempView("BASpread")
    val dfMedianByBondAndDate = spark.sql("""SELECT bond, `date`, percentile_approx(ba_spread, 0.5, 100) AS median_of_tgs
FROM BASpread GROUP BY bond, `date`""")
    dfBASpread.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterBASpread = Cal.getTime().getTime
    tsDelta = ((tsAfterBASpread - tsStart).toDouble/1000)
    println("DBG: getQuotMetrics: tsAfterBASpread: " + tsAfterBASpread + "; delta " + tsDelta)

    // и далее - медиану ото всех дат для бумаги
    dfMedianByBondAndDate.createOrReplaceTempView("MedianByBondAndDate")
    val dfBASpreadRelativeMedian = spark.sql("""SELECT bond, percentile_approx(median_of_tgs, 0.5, 100) AS bid_ask_spread_relative_median
FROM MedianByBondAndDate GROUP BY bond""")
      .persist(MEMORY_ONLY)
    dfMedianByBondAndDate.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterMedianByBondAndDate = Cal.getTime().getTime
    tsDelta = ((tsAfterMedianByBondAndDate - tsAfterBASpread).toDouble/1000)
    println("DBG: getQuotMetrics: tsAfterMedianByBondAndDate: " + tsAfterMedianByBondAndDate + "; delta " + tsDelta)

    // providers_cnt и qdays_cnt для каждой бумаги по поставщикам котировок
    val dfQuotesCnts = quotes
      .groupBy("bond")
      .agg(
        countDistinct("prov").alias("providers_cnt"),
        countDistinct("date").alias("qdays_cnt")
      )
      .select($"bond", $"providers_cnt", $"qdays_cnt")
      .persist(MEMORY_ONLY)

    Cal = Calendar.getInstance()
    val tsAfterCnts = Cal.getTime().getTime
    tsDelta = ((tsAfterCnts - tsAfterMedianByBondAndDate).toDouble/1000)
    println("DBG: getQuotMetrics: tsAfterCnts: " + tsAfterCnts + "; delta " + tsDelta)

    val quotSchema = new StructType()
      .add(StructField("id", LongType, false))
      .add(StructField("isin", StringType, false))
      .add(StructField("bid_ask_spread_relative_median", DoubleType, false))
      .add(StructField("providers_cnt", LongType, false))
      .add(StructField("qdays_cnt", LongType, false))

    val dfRes = bonds.as("L")
      .join(
        dfBASpreadRelativeMedian.as("R"),
        $"R.bond" === $"L.id",
        "inner"
      )
      .join(
        dfQuotesCnts.as("R2"),
        $"R2.bond" === $"L.id",
        "inner"
      )
      .select(
        $"L.id".cast(LongType),
        $"L.isin".cast(StringType),
        $"R.bid_ask_spread_relative_median".cast(DoubleType),
        $"R2.providers_cnt".cast(LongType),
        $"R2.qdays_cnt".cast(LongType)
      )
      //.orderBy($"id".asc)
    dfBASpreadRelativeMedian.unpersist()
    dfQuotesCnts.unpersist()

    val dfResSchematized = spark.createDataFrame(dfRes.rdd, quotSchema)
    dfRes.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsAfterCnts).toDouble/1000)
    println("DBG: getQuotMetrics: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)

    // return
    dfResSchematized
  }


  /**
   *
   */
  def getTsqMetrics(
                     exchangeQuotes: DataFrame,
                     dfBonds: DataFrame
                   ): DataFrame = {
    var tsDelta: Double = 0.0

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime

    println("DBG: getTsqMetrics: tsStart: " + tsStart + "; delta 0")

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. биржевые котировки чистим и накидываем явную схему
    val exchQuotes = exchangeQuotes
      .filter(
        $"bond".isNotNull
        && $"tg".isNotNull
        && $"date".isNotNull
        && $"bid".isNotNull
        && $"ask".isNotNull
        && $"tg".notEqual(327)
        && $"tg".notEqual(381)
      ).as("L")
      .join(
        dfBonds.as("R"),
        $"R.id" === $"L.bond",
        "inner"
      )
      .select(
        $"L.bond".as("bond").cast(LongType),
        $"L.tg".as("prov").cast(LongType),
        $"L.date".as("date").cast(DateType),
        $"L.bid".as("bid").cast(DoubleType),
        $"L.ask".as("ask").cast(DoubleType)
      )
      .persist(MEMORY_ONLY)
    val exchSchema = new StructType()
      .add(StructField("bond", LongType, false))
      .add(StructField("prov", LongType, false))
      .add(StructField("date", DateType, false))
      .add(StructField("bid", DoubleType, false))
      .add(StructField("ask", DoubleType, false))
    val exchQuotesStrict = spark.createDataFrame(exchQuotes.rdd, exchSchema)
      .persist(MEMORY_ONLY)

    exchQuotes.unpersist()
    exchangeQuotes.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterIndataSchematize = Cal.getTime().getTime
    tsDelta = ((tsAfterIndataSchematize - tsStart).toDouble/1000)
    println("DBG: getTsqMetrics: tsAfterIndataSchematize: " + tsAfterIndataSchematize + "; delta " + tsDelta)

    // 2. отдаём в расчётчик и возвращаем
    val quotMetrics: DataFrame = this.getQuotMetrics(exchQuotesStrict, dfBonds)
    exchQuotesStrict.unpersist()

    // return
    quotMetrics
  }

  /**
   *
   */
  def getOtcMetrics(
                     otcQuotes: DataFrame,
                     dfBonds: DataFrame
                   ): DataFrame = {
    var tsDelta: Double = 0.0

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime

    println("DBG: getOtcMetrics: tsStart: " + tsStart + "; delta 0")

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //println("DBG: dfBonds")
    //dfBonds.show(20)

    // 0.1 не надо зря работать по бондам не из листа
    //println("DBG: otcQuotes")
    //otcQuotes.show(20)
    val otcQuotesCleaned = otcQuotes
      .filter(
        $"bond".isNotNull
        && $"org".isNotNull
        && $"date".isNotNull
        && $"price".isNotNull
        && $"price" > 0
      )
      .as("L")
      .join(
        dfBonds.as("R"),
        $"R.id" === $"L.bond",
        "inner"
      ).select(
        $"L.bond".as("bond"),
        $"L.org".as("prov"),
        $"L.date".as("date"),
        $"L.quote_type".as("quote_type"),
        $"L.price".as("price")
      )
    otcQuotes.unpersist()
    //println("DBG: otcQuotesCleaned")
    //otcQuotesCleaned.show(20)

    // 1. внебиржевые котировки приходят bid и ask построчно, пересобираем их в колонки
    val otcQuotesBid = otcQuotesCleaned
      .where(
        $"quote_type" === 1
      ).as("T")
      .select(
        $"T.bond".as("bond").cast(LongType),
        $"T.prov".as("prov").cast(LongType),
        $"T.date".as("date").cast(DateType),
        $"T.price".as("bid").cast(DoubleType)
      )
    //println("DBG: otcQuotesBid")
    //otcQuotesBid.show(20)
    val otcQuotesAsk = otcQuotesCleaned
      .where(
        $"quote_type" === 2
      ).as("T")
      .select(
        $"T.bond".as("bond").cast(LongType),
        $"T.prov".as("prov").cast(LongType),
        $"T.date".as("date").cast(DateType),
        $"T.price".as("ask").cast(DoubleType)
      )
    //println("DBG: otcQuotesAsk")
    //otcQuotesAsk.show(20)
    otcQuotesCleaned.unpersist()
    val otcQuotesAll = otcQuotesBid.as("L")
      .join(
        otcQuotesAsk.as("R"),
        (
          $"R.bond" === $"L.bond"
          && $"R.prov" === $"L.prov"
          && $"R.date" === $"L.date"
        ),
        "inner"
      )
      .select(
        $"L.bond".as("bond"),
        $"L.prov".as("prov"),
        $"L.date".as("date"),
        $"L.bid".as("bid"),
        $"R.ask".as("ask")
      )
      .persist(MEMORY_ONLY)
    otcQuotesBid.unpersist()
    otcQuotesAsk.unpersist()
    //println("DBG: otcQuotesAll")
    //otcQuotesAll.show(20)
    val otcSchema = new StructType()
      .add(StructField("bond", LongType, false))
      .add(StructField("prov", LongType, false))
      .add(StructField("date", DateType, false))
      .add(StructField("bid", DoubleType, false))
      .add(StructField("ask", DoubleType, false))
    val otcQuotesStrict = spark.createDataFrame(otcQuotesAll.rdd, otcSchema)
      .persist(MEMORY_ONLY)
    otcQuotesAll.unpersist()
    //println("DBG: otcQuotesStrict")
    //otcQuotesStrict.show(20)

    Cal = Calendar.getInstance()
    val tsAfterIndataSchematize = Cal.getTime().getTime
    tsDelta = ((tsAfterIndataSchematize - tsStart).toDouble/1000)
    println("DBG: getOtcMetrics: tsAfterIndataSchematize: " + tsAfterIndataSchematize + "; delta " + tsDelta)

    // 2. отдаём в расчётчик и возвращаем
    val quotMetrics: DataFrame = this.getQuotMetrics(otcQuotesStrict, dfBonds)
    otcQuotesStrict.unpersist()
    //println("DBG: quotMetrics")
    //quotMetrics.show(20)

    // return
    quotMetrics
  }

  def toDouble: (Any) => Double = { case i: Int => i case f: Float => f case d: Double => d }

  def mergeQuotesMetrics(
                          tsqMetrics: DataFrame,
                          otcMetrics: DataFrame,
                          dfBonds: DataFrame,
                          dfWeights: DataFrame,
                          anchorDate: String
                        ): DataFrame = {
    var tsDelta: Double = 0.0

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime

    println("DBG: mergeQuotesMetrics: tsStart: " + tsStart + "; delta 0")
    println("DBG: mergeQuotesMetrics: anchorDate: " + anchorDate + "")

    // ====
    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    // ====

    var baspread_rel_rank_w: Double = 0.1
    var providers_cnt_rank_w: Double = 0.1
    var usd_volume_rank_w: Double = 0.1
    var qdays_cnt_rank_w: Double = 0.1
    var exchange_indicator_rank_w: Double = 0.1

    var dfTmp: DataFrame = null
    var topRow: Row = null

    dfTmp = dfWeights.filter($"rank".equalTo("baspread_rel_rank_w")).select($"weight".cast(DoubleType))
    topRow = dfTmp.take(1)(0)
    baspread_rel_rank_w = toDouble(topRow(0))
    //
    dfTmp = dfWeights.filter($"rank".equalTo("providers_cnt_rank_w")).select($"weight".cast(DoubleType))
    topRow = dfTmp.take(1)(0)
    providers_cnt_rank_w = toDouble(topRow(0))
    //
    dfTmp = dfWeights.filter($"rank".equalTo("usd_volume_rank_w")).select($"weight".cast(DoubleType))
    topRow = dfTmp.take(1)(0)
    usd_volume_rank_w = toDouble(topRow(0))
    //
    dfTmp = dfWeights.filter($"rank".equalTo("qdays_cnt_rank_w")).select($"weight".cast(DoubleType))
    topRow = dfTmp.take(1)(0)
    qdays_cnt_rank_w = toDouble(topRow(0))
    //
    dfTmp = dfWeights.filter($"rank".equalTo("exchange_indicator_rank_w")).select($"weight".cast(DoubleType))
    topRow = dfTmp.take(1)(0)
    exchange_indicator_rank_w = toDouble(topRow(0))
    //
    println("DBG: baspread_rel_rank_w: " + baspread_rel_rank_w)
    println("DBG: providers_cnt_rank_w: " + providers_cnt_rank_w)
    println("DBG: usd_volume_rank_w: " + usd_volume_rank_w)
    println("DBG: qdays_cnt_rank_w: " + qdays_cnt_rank_w)
    println("DBG: exchange_indicator_rank_w: " + exchange_indicator_rank_w)

    val mergedQuotesMetrics = dfBonds.as("bonds")
      .join(
        tsqMetrics.as("tsq"),
        $"tsq.id" === $"bonds.id",
        "left"
      )
      .join(
        otcMetrics.as("otc"),
        $"otc.id" === $"bonds.id",
        "left"
      )
      .filter(
        $"tsq.id".isNotNull
        || $"otc.id".isNotNull
      )
      .select(
        $"bonds.id".as("id"),
        $"tsq.id".as("tsq_id"),
        $"bonds.isin".as("isin"),
        $"bonds.usd_volume".as("usd_volume"),
        $"bonds.date_of_end_placing".as("date_of_end_placing"),
        $"bonds.maturity_date".as("maturity_date"),
        $"tsq.bid_ask_spread_relative_median".as("tsq_baspread_rel_median"),
        $"tsq.providers_cnt".as("tsq_providers_cnt"),
        $"tsq.qdays_cnt".as("tsq_qdays_cnt"),
        $"otc.bid_ask_spread_relative_median".as("otc_baspread_rel_median"),
        $"otc.providers_cnt".as("otc_providers_cnt"),
        $"otc.qdays_cnt".as("otc_qdays_cnt")
      )
      .withColumn(
        "anchor_date",
        to_date(lit(anchorDate), "yyyyMMdd")
      )
      .withColumn(
        "baspread_rel_median",
        when(
          $"tsq_baspread_rel_median".isNotNull && $"otc_baspread_rel_median".isNotNull,
          ($"tsq_baspread_rel_median" + $"otc_baspread_rel_median")/2
        )
        .when(
          $"tsq_baspread_rel_median".isNotNull,
          $"tsq_baspread_rel_median"
        )
        .otherwise(
          $"otc_baspread_rel_median"
        )
      )
      .withColumn(
        "providers_cnt",
        when(
          $"tsq_providers_cnt".isNotNull && $"otc_providers_cnt".isNotNull,
          $"tsq_providers_cnt" + $"otc_providers_cnt"
        )
        .when(
          $"tsq_providers_cnt".isNotNull,
          $"tsq_providers_cnt"
        )
        .otherwise(
          $"otc_providers_cnt"
        )
      )
      .withColumn(
        "qdays_cnt",
        when($"tsq_qdays_cnt".isNull, $"otc_qdays_cnt").otherwise($"tsq_qdays_cnt")
      )
      .withColumn(
        "exchange_indicator_rank",
        when($"tsq_id".isNotNull, 1.0).otherwise(0.5)
      )
      .withColumn(
        "days_since_issue",
        datediff($"anchor_date", $"date_of_end_placing")
      )
      .withColumn(
        "days_to_maturity",
        datediff($"maturity_date", $"anchor_date")
      )
      .persist(MEMORY_ONLY)

    //println("DBG: mergedQuotesMetrics")
    //mergedQuotesMetrics.show(20)

    Cal = Calendar.getInstance()
    val tsAfterMerge = Cal.getTime().getTime
    tsDelta = ((tsAfterMerge - tsStart).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterMerge: " + tsAfterMerge + "; delta " + tsDelta)

    println("DBG: mergedQuotesMetrics.rdd.partitions.size: "+ mergedQuotesMetrics.rdd.partitions.size)

    val dfAgg = mergedQuotesMetrics
      .select(
        $"id",
        $"providers_cnt",
        $"qdays_cnt"
      )
      .groupBy()
      .agg(
        max($"providers_cnt").alias("max_providers_cnt"),
        max($"qdays_cnt").alias("max_qdays_cnt"),
        count($"id").alias("total_bonds")
      )
      .select(
        $"max_providers_cnt".cast(LongType),
        $"max_qdays_cnt".cast(LongType),
        $"total_bonds".cast(LongType)
      )
      //.persist(MEMORY_ONLY)

    Cal = Calendar.getInstance()
    val tsAfterAgg = Cal.getTime().getTime
    tsDelta = ((tsAfterAgg - tsAfterMerge).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterAgg: " + tsAfterAgg + "; delta " + tsDelta)

    println("DBG: dfAgg.rdd.partitions.size: "+ dfAgg.rdd.partitions.size)

    /*
    var cntProvidersMax: Long = 0
    var cntQdaysMax: Long = 0
    var cntTotal: Long = 0
    val arrayAgg = dfAgg
      .select(
        $"max_providers_cnt", $"max_qdays_cnt", $"total_bonds"
      )
      .collect()
    //dfAgg.unpersist()
    arrayAgg.foreach { row =>
      cntProvidersMax = row.getLong(0)
      cntQdaysMax = row.getLong(1)
      cntTotal = row.getLong(2)
    }
    println("DBG: mergeQuotesMetrics: cntProvidersMax: " + cntProvidersMax)
    println("DBG: mergeQuotesMetrics: cntQdaysMax: " + cntQdaysMax)
    println("DBG: mergeQuotesMetrics: cntTotal: " + cntTotal)

    Cal = Calendar.getInstance()
    val tsAfterCollect = Cal.getTime().getTime
    tsDelta = ((tsAfterCollect - tsAfterAgg).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterCollect: " + tsAfterCollect + "; delta " + tsDelta)
    */

    val mergedQuotesMetrics2 = mergedQuotesMetrics.coalesce(2)
    println("DBG: mergedQuotesMetrics2.rdd.partitions.size: "+ mergedQuotesMetrics2.rdd.partitions.size)
    Cal = Calendar.getInstance()
    val tsAfterRepartition = Cal.getTime().getTime
    tsDelta = ((tsAfterRepartition - tsAfterAgg).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterRepartition: " + tsAfterRepartition + "; delta " + tsDelta)

    val dfJoined = mergedQuotesMetrics2.crossJoin(dfAgg)
    Cal = Calendar.getInstance()
    val tsAfterCrossJoin = Cal.getTime().getTime
    tsDelta = ((tsAfterCrossJoin - tsAfterRepartition).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterCrossJoin: " + tsAfterCrossJoin + "; delta " + tsDelta)

    //println("DBG: mergeQuotesMetrics: dfJoined")
    //dfJoined.show(20)

    println("DBG: dfJoined.rdd.partitions.size: "+ dfJoined.rdd.partitions.size)

    val dfJoined2 = dfJoined
      .select(
        $"id".cast(LongType),
        //$"tsq_id".cast(LongType),
        $"isin".cast(StringType),
        $"usd_volume".cast(DoubleType),
        $"date_of_end_placing".cast(DateType),
        $"maturity_date".cast(DateType),
        $"tsq_baspread_rel_median".cast(DoubleType),
        $"tsq_providers_cnt".cast(LongType),
        $"tsq_qdays_cnt".cast(LongType),
        $"otc_baspread_rel_median".cast(DoubleType),
        $"otc_providers_cnt".cast(LongType),
        $"otc_qdays_cnt".cast(LongType),
        $"anchor_date".cast(DateType),
        $"baspread_rel_median".cast(DoubleType),
        $"providers_cnt".cast(LongType),
        $"qdays_cnt".cast(LongType),
        $"exchange_indicator_rank".cast(DoubleType),
        $"days_since_issue".cast(LongType),
        $"days_to_maturity".cast(LongType),
        $"max_providers_cnt".cast(LongType),
        $"max_qdays_cnt".cast(LongType),
        $"total_bonds".cast(LongType)
      )
    //println("DBG: mergeQuotesMetrics: dfJoined2:")
    //dfJoined2.show(20)
    /*
    val dfJoined2 = dfJoined.repartition(20)
    */
    Cal = Calendar.getInstance()
    val tsAfterCastedSelect = Cal.getTime().getTime
    tsDelta = ((tsAfterCastedSelect - tsAfterCrossJoin).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterCastedSelect: " + tsAfterCastedSelect + "; delta " + tsDelta)
    println("DBG: dfJoined2.rdd.partitions.size: "+ dfJoined2.rdd.partitions.size)

    val schJoined = new StructType()
      .add(StructField("id", LongType, true))
      //.add(StructField("tsq_id", LongType, true))
      .add(StructField("isin", StringType, true))
      .add(StructField("usd_volume", DoubleType, true))
      .add(StructField("date_of_end_placing", DateType, true))
      .add(StructField("maturity_date", DateType, true))
      .add(StructField("tsq_baspread_rel_median", DoubleType, true))
      .add(StructField("tsq_providers_cnt", LongType, true))
      .add(StructField("tsq_qdays_cnt", LongType, true))
      .add(StructField("otc_baspread_rel_median", DoubleType, true))
      .add(StructField("otc_providers_cnt", LongType, true))
      .add(StructField("otc_qdays_cnt", LongType, true))
      .add(StructField("anchor_date", DateType, false))
      .add(StructField("baspread_rel_median", DoubleType, true))
      .add(StructField("providers_cnt", LongType, true))
      .add(StructField("qdays_cnt", LongType, true))
      .add(StructField("exchange_indicator_rank", DoubleType, true))
      .add(StructField("days_since_issue", LongType, true))
      .add(StructField("days_to_maturity", LongType, true))
      .add(StructField("max_providers_cnt", LongType, true))
      .add(StructField("max_qdays_cnt", LongType, true))
      .add(StructField("total_bonds", LongType, true))
    val dfJoinedAdnShematized = spark.createDataFrame(dfJoined2.rdd, schJoined)

    Cal = Calendar.getInstance()
    val tsAfterSchematizeJoined = Cal.getTime().getTime
    tsDelta = ((tsAfterSchematizeJoined - tsAfterCastedSelect).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterSchematizeJoined: " + tsAfterSchematizeJoined + "; delta " + tsDelta)
    println("DBG: dfJoinedAdnShematized.rdd.partitions.size: "+ dfJoinedAdnShematized.rdd.partitions.size)

    /*
    partitionBy(lit(0)) - против "No Partition Defined for Window operation! Moving all data to a single partition"
    https://stackoverflow.com/questions/41313488/avoid-performance-impact-of-a-single-partition-mode-in-spark-window-functions
    */
    val winRnkBaspread = Window.partitionBy(lit(0)).orderBy($"baspread_rel_median".desc)
    val winRnkVolumeUsd = Window.partitionBy(lit(0)).orderBy($"usd_volume".asc)

    //println("DBG: mergeQuotesMetrics: dfJoinedAdnShematized:")
    //dfJoinedAdnShematized.show(20)

    val dfForLiq = dfJoinedAdnShematized
      //.withColumn("total_bonds", lit(cntTotal))
      .withColumn(
        "pos_by_baspread_rel_median",
        (row_number() over winRnkBaspread)
      )
      .withColumn(
        "baspread_rel_rank",
        ($"pos_by_baspread_rel_median"/$"total_bonds")
      )
      //.withColumn("max_providers_cnt", lit(cntProvidersMax))
      .withColumn(
        "providers_cnt_rank",
        ($"providers_cnt"/$"max_providers_cnt")
      )
      .withColumn(
        "pos_by_usd_volume",
        (row_number() over winRnkVolumeUsd)
      )
      .withColumn(
        "usd_volume_rank",
        ($"pos_by_usd_volume"/$"total_bonds")
      )
      //.withColumn("max_qdays_cnt", lit(cntQdaysMax))
      .withColumn(
        "qdays_cnt_rank",
        when($"qdays_cnt" < 19, 0)
          .otherwise($"qdays_cnt"/$"max_qdays_cnt")
      )
      .withColumn("baspread_rel_rank_w", lit(baspread_rel_rank_w))
      .withColumn("providers_cnt_rank_w", lit(providers_cnt_rank_w))
      .withColumn("usd_volume_rank_w", lit(usd_volume_rank_w))
      .withColumn("qdays_cnt_rank_w", lit(qdays_cnt_rank_w))
      .withColumn("exchange_indicator_rank_w", lit(exchange_indicator_rank_w))
      .select(
        $"id".cast(LongType),
        $"isin".cast(StringType),
        $"date_of_end_placing".cast(DateType),
        $"maturity_date".cast(DateType),
        $"anchor_date".cast(DateType),
        $"days_since_issue".cast(LongType),
        $"days_to_maturity".cast(LongType),
        // total_bonds
        $"total_bonds".cast(LongType),
        // baspread_rel_rank
        $"tsq_baspread_rel_median".cast(DoubleType),
        $"otc_baspread_rel_median".cast(DoubleType),
        $"baspread_rel_median".cast(DoubleType),
        $"pos_by_baspread_rel_median".cast(DoubleType),
        $"baspread_rel_rank".cast(DoubleType),
        $"baspread_rel_rank_w".cast(DoubleType),
        // providers_cnt_rank
        $"tsq_providers_cnt".cast(LongType),
        $"otc_providers_cnt".cast(LongType),
        $"max_providers_cnt".cast(LongType),
        $"providers_cnt".cast(LongType),
        $"providers_cnt_rank".cast(DoubleType),
        $"providers_cnt_rank_w".cast(DoubleType),
        // volume_usd_rank
        $"usd_volume".cast(DoubleType),
        $"pos_by_usd_volume".cast(DoubleType),
        $"usd_volume_rank".cast(DoubleType),
        $"usd_volume_rank_w".cast(DoubleType),
        // qdays_cnt_rank
        $"tsq_qdays_cnt".cast(LongType),
        $"otc_qdays_cnt".cast(LongType),
        $"max_qdays_cnt".cast(LongType),
        $"qdays_cnt".cast(LongType),
        $"qdays_cnt_rank".cast(DoubleType),
        $"qdays_cnt_rank_w".cast(DoubleType),
        // exchange_indicator_rank
        $"exchange_indicator_rank".cast(DoubleType),
        $"exchange_indicator_rank_w".cast(DoubleType)
      )
    //println("DBG: mergeQuotesMetrics: dfForLiq:")
    //dfForLiq.show(20)

    val dfWithRanksAndLiq = dfForLiq
      .withColumn(
        "weights_sum",
        ($"baspread_rel_rank_w" + $"providers_cnt_rank_w"
         + $"usd_volume_rank_w" + $"qdays_cnt_rank_w" + $"exchange_indicator_rank_w"
        ).cast(DoubleType)
      )
      .withColumn(
        "liq_index",
        pow(
          (
              pow($"baspread_rel_rank", $"baspread_rel_rank_w")
            * pow($"providers_cnt_rank", $"providers_cnt_rank_w")
            * pow($"usd_volume_rank", $"usd_volume_rank_w")
            * pow($"qdays_cnt_rank", $"qdays_cnt_rank_w")
            * pow($"exchange_indicator_rank", $"exchange_indicator_rank_w")
          )
          , pow(
              (
                  $"baspread_rel_rank_w"
                + $"providers_cnt_rank_w"
                + $"usd_volume_rank_w"
                + $"qdays_cnt_rank_w"
                + $"exchange_indicator_rank_w"
              )
              , -1
            )
        ).cast(DoubleType)
        /*
        exp(
          pow($"weights_sum", -1)
          * (
              when($"baspread_rel_rank".notEqual(0), $"baspread_rel_rank_w" * log($"baspread_rel_rank")).otherwise(0)
            + when($"providers_cnt_rank".notEqual(0), $"providers_cnt_rank_w" * log($"providers_cnt_rank")).otherwise(0)
            + when($"usd_volume_rank".notEqual(0), $"usd_volume_rank_w" * log($"usd_volume_rank")).otherwise(0)
            + when($"qdays_cnt_rank".notEqual(0), $"qdays_cnt_rank_w" * log($"qdays_cnt_rank")).otherwise(0)
            + when($"exchange_indicator_rank".notEqual(0), $"exchange_indicator_rank_w" * log($"exchange_indicator_rank")).otherwise(0)
          )
        )
        */
      )
      .select(
        $"id".cast(LongType),
        $"isin".cast(StringType),
        $"date_of_end_placing".cast(DateType),
        $"maturity_date".cast(DateType),
        $"anchor_date".cast(DateType),
        $"days_since_issue".cast(LongType),
        $"days_to_maturity".cast(LongType),
        // total_bonds
        $"total_bonds".cast(LongType),
        // baspread_rel_rank
        $"tsq_baspread_rel_median".cast(DoubleType),
        $"otc_baspread_rel_median".cast(DoubleType),
        $"baspread_rel_median".cast(DoubleType),
        $"pos_by_baspread_rel_median".cast(LongType),
        $"baspread_rel_rank".cast(DoubleType),
        $"baspread_rel_rank_w".cast(DoubleType),
        // providers_cnt_rank
        $"tsq_providers_cnt".cast(LongType),
        $"otc_providers_cnt".cast(LongType),
        $"max_providers_cnt".cast(LongType),
        $"providers_cnt".cast(LongType),
        $"providers_cnt_rank".cast(DoubleType),
        $"providers_cnt_rank_w".cast(DoubleType),
        // volume_usd_rank
        $"usd_volume".cast(DoubleType),
        $"pos_by_usd_volume".cast(LongType),
        $"usd_volume_rank".cast(DoubleType),
        $"usd_volume_rank_w".cast(DoubleType),
        // qdays_cnt_rank
        $"tsq_qdays_cnt".cast(LongType),
        $"otc_qdays_cnt".cast(LongType),
        $"max_qdays_cnt".cast(LongType),
        $"qdays_cnt".cast(LongType),
        $"qdays_cnt_rank".cast(DoubleType),
        $"qdays_cnt_rank_w".cast(DoubleType),
        // exchange_indicator_rank
        $"exchange_indicator_rank".cast(DoubleType),
        $"exchange_indicator_rank_w".cast(DoubleType),
        //
        $"liq_index".cast(DoubleType)
      )
      .persist(MEMORY_ONLY)

    mergedQuotesMetrics.unpersist()
    dfAgg.unpersist()

    //println("DBG: mergeQuotesMetrics: dfWithRanksAndLiq:")
    //dfWithRanksAndLiq.show(20)

    Cal = Calendar.getInstance()
    val tsAfterLiqs = Cal.getTime().getTime
    tsDelta = ((tsAfterLiqs - tsAfterSchematizeJoined).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterLiqs: " + tsAfterLiqs + "; delta " + tsDelta)

    val dfSchema = new StructType()
      .add(StructField("id", LongType, false))
      .add(StructField("isin", StringType, false))
      .add(StructField("date_of_end_placing", DateType, true))
      .add(StructField("maturity_date", DateType, true))
      .add(StructField("anchor_date", DateType, false))
      .add(StructField("days_since_issue", LongType, true))
      .add(StructField("days_to_maturity", LongType, true))
      .add(StructField("total_bonds", LongType, true))
      .add(StructField("tsq_baspread_rel_median", DoubleType, true))
      .add(StructField("otc_baspread_rel_median", DoubleType, true))
      .add(StructField("baspread_rel_median", DoubleType, true))
      .add(StructField("pos_by_baspread_rel_median", LongType, true))
      .add(StructField("baspread_rel_rank", DoubleType, true))
      .add(StructField("baspread_rel_rank_w", DoubleType, true))
      .add(StructField("tsq_providers_cnt", LongType, true))
      .add(StructField("otc_providers_cnt", LongType, true))
      .add(StructField("max_providers_cnt", LongType, true))
      .add(StructField("providers_cnt", LongType, true))
      .add(StructField("providers_cnt_rank", DoubleType, true))
      .add(StructField("providers_cnt_rank_w", DoubleType, true))
      .add(StructField("usd_volume", DoubleType, true))
      .add(StructField("pos_by_usd_volume", LongType, true))
      .add(StructField("usd_volume_rank", DoubleType, true))
      .add(StructField("usd_volume_rank_w", DoubleType, true))
      .add(StructField("tsq_qdays_cnt", LongType, true))
      .add(StructField("otc_qdays_cnt", LongType, true))
      .add(StructField("max_qdays_cnt", LongType, true))
      .add(StructField("qdays_cnt", LongType, true))
      .add(StructField("qdays_cnt_rank", DoubleType, true))
      .add(StructField("qdays_cnt_rank_w", DoubleType, true))
      .add(StructField("exchange_indicator_rank", DoubleType, true))
      .add(StructField("exchange_indicator_rank_w", DoubleType, true))
      .add(StructField("liq_index", DoubleType, true))
    val dfRes = spark.createDataFrame(dfWithRanksAndLiq.rdd, dfSchema)
    //  .persist(MEMORY_ONLY)

    //println("DBG: mergeQuotesMetrics: dfRes:")
    //dfRes.show(20)

    dfWithRanksAndLiq.unpersist()

    Cal = Calendar.getInstance()
    val tsAfterAll = Cal.getTime().getTime
    tsDelta = ((tsAfterAll - tsAfterLiqs).toDouble/1000)
    println("DBG: mergeQuotesMetrics: tsAfterAll: " + tsAfterAll + "; delta " + tsDelta)

    // return
    dfRes
  }
}
