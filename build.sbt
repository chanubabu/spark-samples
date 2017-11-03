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

// https://mvnrepository.com/artifact/com.twitter/bijection-core_2.10
libraryDependencies += "com.twitter" % "bijection-core_2.10" % "0.5.3"
// https://mvnrepository.com/artifact/com.twitter/bijection-avro_2.10
libraryDependencies += "com.twitter" % "bijection-avro_2.10" % "0.7.0"

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.2.1"


libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.1.1" % "provided"
