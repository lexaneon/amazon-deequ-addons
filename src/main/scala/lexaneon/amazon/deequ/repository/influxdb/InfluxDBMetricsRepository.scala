package lexaneon.amazon.deequ.repository.influxdb

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.influxdb.{InfluxDB, InfluxDBFactory}

import java.util.concurrent.TimeUnit

/** A Repository implementation using an influxDB
 *
 * @param serverURLWithPort - influxDB server URL with port, example: http://localhost:8086
 * @param dbName
 * @param measurementName
 */
class InfluxDBMetricsRepository(influxDBConnectionProperties: InfluxDBConnectionProperties) extends MetricsRepository{

  implicit val influxDBConnect = initInfluxDBConnect

  def initInfluxDBConnect(): InfluxDB = {
    val influxDB = InfluxDBFactory.connect(influxDBConnectionProperties.serverURLWithPort)
    influxDB.setDatabase(influxDBConnectionProperties.dbName)
    influxDB
  }

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)
    val points =
      InfluxDBAnalysisResultSerde.
        analysisResultToInfluxPointObject(resultKey, analyzerContextWithSuccessfulValues, influxDBConnectionProperties.measurementName)

    points.foreach(point => InfluxDBMetricsRepositoryMultipleResultsLoader.writeToInfluxDB(point))

  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = None

  override def load(): MetricsRepositoryMultipleResultsLoader = new InfluxDBMetricsRepositoryMultipleResultsLoader // TODO should be finished
}
