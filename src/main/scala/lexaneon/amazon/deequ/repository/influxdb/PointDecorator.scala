package lexaneon.amazon.deequ.repository.influxdb

import org.influxdb.dto.Point

import java.{lang, util}
import java.util.concurrent.TimeUnit

object PointDecorator {
  def measurement(measurement: String): PointBuilderDecorator = new PointBuilderDecorator(Point.measurement(measurement))
}

class PointBuilderDecorator(val pointBuilder: Point.Builder) {
  def tag(tagName: String, value: String): PointBuilderDecorator = {
    pointBuilder.tag(tagName, value)
    this
  }

  def tag(tagsToAdd: util.Map[String, String]): PointBuilderDecorator = {
    pointBuilder.tag(tagsToAdd)
    this
  }

  def addField(field: String, value: Boolean): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Long): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Double): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Int): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Float): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Short): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Number): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: String): PointBuilderDecorator = {
    pointBuilder.addField(field, value)
    this
  }

  def addField(field: String, value: Any): PointBuilderDecorator = {
    value match{
      case k: Double =>  pointBuilder.addField(field, k)
      case k: Float => pointBuilder.addField(field, k)
      case k: Long =>  pointBuilder.addField(field, k)
      case k: Int => pointBuilder.addField(field, k)
      case k: Short =>  pointBuilder.addField(field, k)
      case k: Byte =>  pointBuilder.addField(field, k)
      case k: Char =>  pointBuilder.addField(field, k)
      case k: Boolean =>  pointBuilder.addField(field, k)
      case k: String =>  pointBuilder.addField(field, k)
      case _ => throw new ClassCastException("No such class to cast")
    }
    this
  }

  def fields(fieldsToAdd: util.Map[String, AnyRef]): PointBuilderDecorator = {
    pointBuilder.fields(fieldsToAdd)
    this
  }

  def time(timeToSet: Number, precisionToSet: TimeUnit): PointBuilderDecorator = {
    pointBuilder.time(timeToSet, precisionToSet)
    this
  }

  def time(timeToSet: Long, precisionToSet: TimeUnit): PointBuilderDecorator = {
    pointBuilder.time(timeToSet, precisionToSet)
    this
  }

  def time(timeToSet: lang.Long, precisionToSet: TimeUnit): PointBuilderDecorator = {
    pointBuilder.time(timeToSet, precisionToSet)
    this
  }

  def hasFields: Boolean = pointBuilder.hasFields

  def addFieldsFromPOJO(pojo: Any): PointBuilderDecorator = {
    pointBuilder.addFieldsFromPOJO(pojo)
    this
  }

  def build(): Point = pointBuilder.build()
}