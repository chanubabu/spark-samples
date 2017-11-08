import java.util.Collections
import javassist.bytecode.stackmap.TypeTag

//import org.apache.kafka.common.utils.Utils

//import com.sun.javafx.util.Utils
import com.typesafe.config.{Config, ConfigFactory}
//import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericData
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

  // This could also be your auto-generated Avro class/type
  case class Foo(s: String)

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

  val schemaStringkkk = """{
                            "type": "record",
                            "name": "Reservations",
                            "fields": [
                              { "name": "Key", "type": "int" },
                              { "name": "Value", "type": "string" }
                            ]
                       }"""

  val schemaString2 = """{
                            "type": "record",
                            "name": "Reservations",
                            "fields": [
                              { "name": "Key", "type": "int" },
                              { "name": "Value", "type": {
                              "type": "record",
                              "name": "Content",
                              "fields": [
                                { "name": "EventSource", "type": "string" },
                                { "name": "KeysOfInterest", "type": "string" },
                                { "name": "EventIdentifier", "type": "string" },
                                { "name": "TimeOfEvent", "type": "string" },
                                { "name": "EventType", "type": "string" },
                                { "name": "Body", "type": "string" }
                              ]
                         } }
                            ]
                       }"""
  val messageSchema = new Schema.Parser().parse(schemaString)
  //val messageSchema1 = new Schema.Parser().parse(schemaString1)
  val reader = new GenericDatumReader[GenericRecord](messageSchema)
  //val reader1 = new GenericDatumReader[GenericRecord](messageSchema1)
  val messageSchema1 = new Schema.Parser().parse(schemaString1)
  val reader1 = new GenericDatumReader[GenericRecord](messageSchema1)

  val messageSchema2 = new Schema.Parser().parse(schemaString,schemaString2)
  val reader2 = new GenericDatumReader[GenericRecord](messageSchema2)
  // Factory to deserialize binary avro data
  //val avroDecoderFactory = DecoderFactory.get()
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
    /*.as[KafkaMessage]
    //.map(msg => KafkaAvroConsumer.transform[T](msg, schemaString1))
    .select($"value".as[Array[Byte]])
    .map(func = d => {
      //val rec = reader.read(null, avroDecoderFactory.binaryDecoder(d, null))
      //val deviceId = rec.get("deviceId").asInstanceOf[Int]
      //val deviceName = rec.get("deviceName").asInstanceOf[org.apache.avro.util.Utf8].toString
      //new KafkaMessage(deviceId, deviceName)
      //{deviceId,deviceName}
      //rec
      //val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](messageSchema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(d, null)
      val userData: GenericRecord = reader.read(null, decoder)

      //val kt = userData.get("Key").asInstanceOf[Int]
      val tt = userData.get("Value").asInstanceOf[org.apache.avro.util.Utf8].getBytes


      //val reader1: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](messageSchema1)
      val decoder1: Decoder = DecoderFactory.get().binaryDecoder(tt, null)
      val userData1: GenericRecord = reader1.read(null, decoder1)

      //val tt1 = userData1.get("EventType").asInstanceOf[org.apache.avro.util.Utf8].toString

      userData1

    }
    )*/

  ds1.printSchema()

  /*implicit val myFooEncoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]
  val foos = ds1.map(row => Foo(new String(row.getAs[Array[Byte]]("value"))))

  foos.foreachPartition{records =>
    records.foreach(
      record => {
        val decoder1: Decoder = DecoderFactory.get().binaryDecoder(record, null)
        val userData1: GenericRecord = reader1.read(null, decoder1)
        userData1
      }
    )
  }*/

  /*val lst =  foos.foreachPartition({ d=>
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(d, null)
    val userData1: GenericRecord = reader.read(null, decoder)
    userData1
  })

  if(lst.hasNext) {
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(lst.next(), null)
    val userData1: GenericRecord = reader.read(null, decoder)

  }*/
  /*def decodeMessages(iter: Iterator[KafkaMessage], schemaRegistryUrl: String) : Iterator[<SomeObject>] = {
    val decoder = AvroTo<YourObject>Decoder.getDecoder(schemaRegistryUrl)
    iter.map(message => {
      val record = decoder.fromBytes(message.value).asInstanceOf[GenericData.Record]
      val field1 = record.get("field1Name").asInstanceOf[GenericData.Record]
      val field2 = record.get("field1Name").asInstanceOf[GenericData.String]
      //create an object with the fields extracted from genericRecord
    })
  }

  val decodedDs  = ds.mapPartitions(decodeMessages(_, schemaString1))*/

  /*val record = decoder.fromBytes(message.value).asInstanceOf[GenericData.Record]
  val field1 = record.get("field1Name").asInstanceOf[GenericData.Record]
  val field2 = record.get("field1Name").asInstanceOf[GenericData.String] */

  //val ds2 = ds1.collect().foreach(r=> r.get("deviceID").asInstanceOf[Int])

  //val ds2 = ds1.col("")

  /*import org.apache.avro.file.DataFileStream
 import org.apache.avro.generic.GenericRecord

 import java.io.ByteArrayInputStream

 val bytes =  ds1.select($"value".as[Array[Byte]])
 val byteArrayInputStream = new ByteArrayInputStream(bytes)
 val dataFileReader = new DataFileStream[GenericRecord](byteArrayInputStream, reader)
*/
  //case class Record(key:Int,Value:Array[Byte])

  //implicit val encoder1: Encoder[Record] = org.apache.spark.sql.Encoders.kryo[Record]
  /*val df = ds1.as[Record].map(func= x => {

    if(x.Value.length > 0)
    {
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(x.Value, null)
      val userData: GenericRecord = reader2.read(null, decoder)
      val content = userData.get("Value").asInstanceOf[org.apache.avro.util.Utf8].getBytes

      val decoder1: Decoder = DecoderFactory.get().binaryDecoder(content, null)
      val userData1: GenericRecord = reader.read(null, decoder1)

      //val et = userData1.get("EventType").asInstanceOf[org.apache.avro.util.Utf8].toString
      //val es = userData1.get("EventSource").asInstanceOf[org.apache.avro.util.Utf8].toString
      //val koi = userData1.get("KeysOfInterest").asInstanceOf[org.apache.avro.util.Utf8].toString
      //val ei = userData1.get("EventIdentifier").asInstanceOf[org.apache.avro.util.Utf8].toString
      //val toi = userData1.get("TimeOfEvent").asInstanceOf[org.apache.avro.util.Utf8].toString
      val body = userData1.get("Body").asInstanceOf[org.apache.avro.util.Utf8].toString

      //et + ":" + es + ":" + ":" + koi + ":" + body
      body
      //kt
      //Value
    }
  })*/
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
