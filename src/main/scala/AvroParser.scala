import org.apache.avro.io.{DatumReader, Decoder}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.collection.immutable.Range.Int

//import com.databricks.spark.avro.SchemaConverters
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql._
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

object AvroParser extends App {

  case class KafkaMessage(key: String, value: Array[Byte],topic: String, partition: Int, offset: Long, timestamp: Timestamp, timestampType: Int)

  //case class KafkaMessage (offset: BigInt, value: Int, timestampType: String, key: Int, topic: String, partition: Int, timestamp: String)
  //case class KafkaMessage (key: binary, value: Int, timestampType: String, key: Int, topic: String, partition: Int, timestamp: String)
  val schemaString1 = """{
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


  val schemaString = """{
                            "type": "record",
                            "name": "Reservations",
                            "fields": [
                              { "name": "Key", "type": "int" },
                              { "name": "Value", "type": "string" }
                            ]
                       }""""

  val messageSchema = new Schema.Parser().parse(schemaString)
  val messageSchema1 = new Schema.Parser().parse(schemaString1)
  val reader = new GenericDatumReader[GenericRecord](messageSchema)
  val reader1 = new GenericDatumReader[GenericRecord](messageSchema1)

  // Factory to deserialize binary avro data
  val avroDecoderFactory = DecoderFactory.get()
  // Register implicit encoder for map operation
  implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]


  val spark = SparkSession
                .builder()
                .appName("Kafka Avro Streaming")
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

  ds1.printSchema()

  /*val record = decoder.fromBytes(message.value).asInstanceOf[GenericData.Record]
  val field1 = record.get("field1Name").asInstanceOf[GenericData.Record]
  val field2 = record.get("field1Name").asInstanceOf[GenericData.String] */

  //val ds2 = ds1.collect().foreach(r=> r.get("deviceID").asInstanceOf[Int])

  //val ds2 = ds1.col("")

  val query = ds1.writeStream
    .outputMode("append")
    .queryName("table")
    .format("console")
    .start()

  query.awaitTermination()

  //spark.sql("select * from table").show(truncate = false)
  //val x = spark.sql("select * from table")

  spark.stop()


}
