import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.11"
  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.8.0"
  lazy val kafkaStreams = "org.apache.kafka" % "kafka-streams" % "2.8.0"
  lazy val kafkaStreamsScala =
    "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0"
  lazy val circe = "io.circe" %% "circe-core" % "0.14.1"
  lazy val circeG = "io.circe" %% "circe-generic" % "0.14.1"
  lazy val circeP = "io.circe" %% "circe-parser" % "0.14.1"

}
