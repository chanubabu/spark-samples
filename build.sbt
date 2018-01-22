
name := "KafkaStructreStream"

version := "0.1"

scalaVersion := "2.11.0"

val kafkaVersion="0.10.0.0" //use kafka-10 or above as broker

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val sparkV = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "com.couchbase.client" %% "spark-connector" % sparkV
  )
}

libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.2.11"

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

// https://mvnrepository.com/artifact/com.twitter/bijection-core_2.10
//libraryDependencies += "com.twitter" % "bijection-core_2.10" % "0.5.3"
// https://mvnrepository.com/artifact/com.twitter/bijection-avro_2.10
//libraryDependencies += "com.twitter" % "bijection-avro_2.10" % "0.7.0"

// https://mvnrepository.com/artifact/com.typesafe/config
//libraryDependencies += "com.typesafe" % "config" % "1.2.1"

//libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "3.0.0"
