name := "amazon-deequ-addons"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.amazon.deequ" % "deequ" % "1.1.0_spark-2.4-scala-2.11" % "provided"
libraryDependencies += "org.influxdb" % "influxdb-java" % "2.21"