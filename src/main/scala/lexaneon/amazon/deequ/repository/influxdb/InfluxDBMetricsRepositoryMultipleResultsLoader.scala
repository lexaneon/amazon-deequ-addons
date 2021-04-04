package lexaneon.amazon.deequ.repository.influxdb

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query}


class InfluxDBMetricsRepositoryMultipleResultsLoader extends MetricsRepositoryMultipleResultsLoader{

  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[Analyzer[_, Metric[_]]]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
   * Filter out results that don't have specific values for specific tags
   *
   * @param tagValues Map with tag names and the corresponding values to filter for
   */
  def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
   * Choose all metrics that you want to load
   *
   * @param analyzers A sequence of analyers who's resulting metrics you want to load
   */
  def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]])
  : MetricsRepositoryMultipleResultsLoader = {

    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
   * Only look at AnalysisResults with a result key with a smaller value
   *
   * @param dateTime The maximum dateTime of AnalysisResults to look at
   */
  def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
   * Only look at AnalysisResults with a result key with a greater value
   *
   * @param dateTime The minimum dateTime of AnalysisResults to look at
   */
  def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /** Get the AnalysisResult */
  override def get(): Seq[AnalysisResult] = Seq.empty[AnalysisResult] //TODO should be finished


}

object InfluxDBMetricsRepositoryMultipleResultsLoader {

  def apply(influxDBConnectionProperties: InfluxDBConnectionProperties): InfluxDBMetricsRepositoryMultipleResultsLoader = {
    new InfluxDBMetricsRepositoryMultipleResultsLoader
  }

  def writeToInfluxDB(point: PointBuilderDecorator)(implicit influxDBConnect: InfluxDB): Unit = {
//    import org.influxdb.BatchOptions TODO delete
//    influxDBConnect.enableBatch(BatchOptions.DEFAULTS)
    influxDBConnect.write(point.build())
  }

  def readFromInfluxDB(implicit influxDBConnect: InfluxDB, measurementName: String): Option[Seq[AnalysisResult]] = {
    val queryResult = influxDBConnect.query(new Query(s"select * from ${measurementName}"))
    InfluxDBAnalysisResultSerde.queryToAnalysisResult(queryResult)
  }
}


