package lexaneon.amazon.deequ.repository.influxdb

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, ResultKey}
import org.influxdb.dto.QueryResult

import java.util.concurrent.TimeUnit

object InfluxDBAnalysisResultSerde {

  val DATASET_DATE_FIELD = "dataSetDate"
  val TAGS_PREFIX_FIELD = "tags_"
  val ENTITY_FIELD = "entity"
  val INSTANCE_FIELD = "instance"
  val NAME_FIELD = "name"
  val VALUE_FIELD = "value"

  // create InfluxDB Point objects from AnalysisResult
  def analysisResultToInfluxPointObject(resultKey: ResultKey, analyzerContext: AnalyzerContext, measurementName: String): Seq[PointBuilderDecorator] = {
    val result =
      analyzerContext
        .allMetrics
        .map(el =>{
          val point = PointDecorator
            .measurement(measurementName)
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField(DATASET_DATE_FIELD, resultKey.dataSetDate)
            .addField(VALUE_FIELD, el.value.toOption.get)
            .tag(ENTITY_FIELD, el.entity.toString)
            .tag(INSTANCE_FIELD, el.instance)
            .tag(NAME_FIELD, el.name)

          resultKey
            .tags
            .foldLeft(point)((acc, pair) => acc.tag(s"$TAGS_PREFIX_FIELD${pair._1}", pair._2))
        })
    result
  }

  def queryToAnalysisResult(queryResult: QueryResult): Option[Seq[AnalysisResult]] = {
    val columns = getColumnsFromQueryResult(queryResult)
    val result = getResultMapFromQueryResult(queryResult)

    // TODO should be finished
    println(s"Columns: ${columns.mkString(";")}")

    result.foreach(rec => {
      rec.foreach(el => print(s"key: ${el._1} value: ${el._2};"))
      println
    })
    val tagColumns = columns.filter(_.startsWith(TAGS_PREFIX_FIELD))

    val resultTable =
      result
        .groupBy(el => {
          val tagMap = tagColumns.map(tags => tags.split(TAGS_PREFIX_FIELD)(1) -> el(tags).asInstanceOf[String]).toMap
          val dataSetMap = Map(DATASET_DATE_FIELD -> el(DATASET_DATE_FIELD).asInstanceOf[String])
          tagMap ++ dataSetMap
        })
        .map(el =>
          (ResultKey(
            el._1(DATASET_DATE_FIELD).asInstanceOf[Long],
            el._1.filter(key => key._1 != DATASET_DATE_FIELD)), el._2.map(k => k).distinct)
        )


    val resultKey =
      result
        .map(
          record =>
            ResultKey(
              record(DATASET_DATE_FIELD).asInstanceOf[Long],
              tagColumns.map(tags => tags.split(TAGS_PREFIX_FIELD)(1) -> record(tags).asInstanceOf[String]).toMap)
        )
        .distinct

    //    val metricMap = TODO finish
    //      result
    //        .map(
    //          record =>
    //            DoubleMetric(
    //              Entity.withName(record(ENTITY_FIELD).asInstanceOf[String]),
    //              record(NAME_FIELD).asInstanceOf[String],
    //              record(INSTANCE_FIELD).asInstanceOf[String],
    //              Try((VALUE_FIELD).asInstanceOf[Double])))
    //    AnalysisResult(resultKey(0), AnalyzerContext(metricMap))
    None
  }
  private def getSeriesFromQueryResult(queryResult: QueryResult): Seq[QueryResult.Series] = {
    import collection.JavaConverters._
    queryResult
      .getResults
      .asScala
      .map(record => {
        record
          .getSeries
          .asScala
      })
      .flatten
  }

  private def getColumnsFromQueryResult(queryResult: QueryResult): Seq[String] = {
    import collection.JavaConverters._
    val series = getSeriesFromQueryResult(queryResult)

    series.map(el => el.getColumns.asScala).flatten
  }

  private def getResultMapFromQueryResult(queryResult: QueryResult): Seq[Map[String, AnyRef]] = {
    import collection.JavaConverters._
    val series = getSeriesFromQueryResult(queryResult)
    series
      .map(el =>
        el
          .getValues
          .asScala
          .map(values =>
            el
              .getColumns
              .asScala
              .zip(values.asScala).toMap)
      ).flatten
  }

}
