package lexaneon.amazon.deequ.example

import com.amazon.deequ.analyzers.{Completeness, CountDistinct, Distinctness, Size, Uniqueness}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.repository.ResultKey
import lexaneon.amazon.deequ.repository.influxdb.{InfluxDBConnectionProperties, InfluxDBMetricsRepository}
import org.apache.spark.sql.SparkSession

object InfluxDBMetricRepository extends App{

  val spark = initSpark()
  val filePath = "src/main/resources/dataForExample/data.csv"
  val df = spark.read.option("header", "true").csv(filePath).toDF()

  val influxDBConnectionProperties = InfluxDBConnectionProperties("http://localhost:8086", "example", "InfluxDBMetricsRepository")

  val resultKey = ResultKey(
    System.currentTimeMillis(),
    Map("dataSetFilePath" -> filePath, "dataSetName" -> "orders"))

  val analysisResult = AnalysisRunner
    .onData(df)
    .useRepository(new InfluxDBMetricsRepository(influxDBConnectionProperties))
    .saveOrAppendResult(resultKey)
    .addAnalyzer(Size())
    .addAnalyzer(Distinctness("customer_id"))
    .addAnalyzer(CountDistinct("customer_id"))
    .addAnalyzer(Uniqueness(Seq("customer_id", "id")))
    .addAnalyzer(Uniqueness("id"))
    .addAnalyzer(Completeness("trans_date"))
    .addAnalyzer(Completeness("id"))
    .run()

  val metric = successMetricsAsDataFrame( spark, analysisResult)

  metric.show(false)

  spark.close()
  def initSpark(isLocalRun: Boolean = true): SparkSession = {
    val sparkSessionBuilder =
      SparkSession
        .builder
        .appName(this.getClass.getSimpleName)

    val spark =
      if (isLocalRun){
        sparkSessionBuilder
          .master("local[*]")
          .getOrCreate()
      }else
        sparkSessionBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark
  }

}


