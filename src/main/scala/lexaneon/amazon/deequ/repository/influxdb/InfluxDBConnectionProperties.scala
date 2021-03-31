package lexaneon.amazon.deequ.repository.influxdb

case class InfluxDBConnectionProperties(
                                         serverURLWithPort: String,
                                         dbName: String,
                                         measurementName: String)
