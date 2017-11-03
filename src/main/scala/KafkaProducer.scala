import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

import scala.collection.mutable

class KafkaSink(topic: String, servers: String) extends ForeachWriter[(String,String)] {

  val kafkaProperties = new Properties()
  kafkaProperties.put("bootstrap.servers",servers)
  kafkaProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val results = new mutable.HashMap[String,String]
  var producer:KafkaProducer[String,String] =_

  def open(partitionId: Long, version: Long): Boolean ={
      producer = new KafkaProducer(kafkaProperties)
      true
  }

  def process(value: (String, String)): Unit = {
    producer.send(new ProducerRecord(topic,value._1+":"+value._2))
  }

  def close(errorOrNull: Throwable): Unit = {
    producer.close()
  }
}

object KafkaProducer extends App{

  val spark = SparkSession
    .builder()
    .appName("Kafka AvroMessage Producer")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Subscribe to 1 topic
  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .load()
  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]

  //make it a streaming dataset
  val lines = spark.read.option("inferSchema","true").option("header","true").csv("/work/learn/users.csv")

  //ds1.printSchema()

  val query = df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "test1")
    .option("checkpointLocation","/work/checkpoints")
    .start()

  query.awaitTermination()

  spark.stop()

}
