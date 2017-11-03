import java.util.Collections

import AvroParser.{messageSchema, messageSchema1}
import com.typesafe.config.{Config, ConfigFactory}
//import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._

import AvroParser.{messageSchema, schemaString}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

object KafkaAvroConsumer extends App{


  case class KafkaMessage(key: String, value: Array[Byte],topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)

 /* |-- key: binary (nullable = true)
  |-- value: binary (nullable = true)
  |-- topic: string (nullable = true)
  |-- partition: integer (nullable = true)
  |-- offset: long (nullable = true)
  |-- timestamp: timestamp (nullable = true)
  |-- timestampType: integer (nullable = true)*/

  val schemaString = """{
                            "type": "record",
                            "name": "Reservations",
                            "fields": [
                              { "name": "EventSource", "type": "string" },
                              { "name": "KeysOfInterest", "type": "string" },
                              { "name": "EventIdentifier", "type": "string" },
                              { "name": "TimeOfEvent", "type": "string" },
                              { "name": "EventType", "type": "string" },
                              { "name": "Body", "type": "string" }
                            ]
                       }""""

  val messageSchema = new Schema.Parser().parse(schemaString)
  val reader = new GenericDatumReader[GenericRecord](messageSchema)
  // Register implicit encoder for map operation
  implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]

 /* private val conf: Config = ConfigFactory.load().getConfig("kafka.consumer")
  val valueDeserializer = new KafkaAvroDeserializer()
  valueDeserializer.configure(Collections.singletonMap("schema.registry.url",
    conf.getString("schema.registry.url")), false)

  def transform[T <: GenericRecord : TypeTag](msg: KafkaMessage, schemaStr: String) = {
    val schema = new Schema.Parser().parse(schemaStr)
    Utils.convert[T](schema)(valueDeserializer.deserialize(msg.topic, msg.value))
  }*/

  val spark = SparkSession
    .builder()
    .appName("Kafka AvroMessage Streaming")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val ds1 = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .as[KafkaMessage]
    .select($"value".as[Array[Byte]])
    .map(func = d => {
      //val rec = reader.read(null, avroDecoderFactory.binaryDecoder(d, null))
      //val deviceId = rec.get("deviceId").asInstanceOf[Int]
      //val deviceName = rec.get("deviceName").asInstanceOf[org.apache.avro.util.Utf8].toString
      //new KafkaMessage(deviceId, deviceName)
      //{deviceId,deviceName}
      //rec
      val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](messageSchema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(d, null)
      val userData: GenericRecord = reader.read(null, decoder)

      //val kt = userData.get("Key").asInstanceOf[Int]
      val tt = userData.get("Value").asInstanceOf[org.apache.avro.util.Utf8].getBytes


      val reader1: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](messageSchema1)
      val decoder1: Decoder = DecoderFactory.get().binaryDecoder(tt, null)
      val userData1: GenericRecord = reader1.read(null, decoder1)

      //val tt1 = userData1.get("EventType").asInstanceOf[org.apache.avro.util.Utf8].toString

      userData1

    }
    )
  //ds1.printSchema()


  val query = ds1.writeStream
    .outputMode("append")
    .queryName("table")
    .format("console")
    .start()

  query.awaitTermination()

  spark.stop()
}
