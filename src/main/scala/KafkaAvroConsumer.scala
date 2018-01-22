import java.io.EOFException

//import com.twitter.bijection.avro.GenericAvroCodecs
import AvroParser.KafkaMessage
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.apache.spark.sql.functions.from_json
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp


object KafkaAvroConsumer extends App {


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
                            "name": "reservations",
                            "fields": [
                              { "name": "EventSource", "type": "string" },
                              { "name": "KeysOfInterest", "type": "string" },
                              { "name": "EventIdentifier", "type": "string" },
                              { "name": "TimeOfEvent", "type": "string" },
                              { "name": "EventType", "type": "string" },
                              { "name": "Body", "type": "string" }
                            ]
                       }"""



  val messageSchema = new Schema.Parser().parse(schemaString)
  val reader = new GenericDatumReader[GenericRecord](messageSchema)

  // Register implicit encoder for map operation
  implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]

  import scala.reflect.ClassTag
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

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
    .option("subscribe", "resCollationEventTopic")
    .option("startingOffsets", "earliest")
    .load()
    .as[KafkaMessage]
    .select($"value".as[Array[Byte]])
    .map(msg => {

    if (msg.iterator.hasNext) {

      val decoder1: Decoder = DecoderFactory.get().binaryDecoder(msg, null)
      val userData1: GenericRecord = reader.read(null, decoder1)

      val et = userData1.get("EventType").asInstanceOf[org.apache.avro.util.Utf8].toString
      val es = userData1.get("EventSource").asInstanceOf[org.apache.avro.util.Utf8].toString
      val koi = userData1.get("KeysOfInterest").asInstanceOf[org.apache.avro.util.Utf8].toString
      val ei = userData1.get("EventIdentifier").asInstanceOf[org.apache.avro.util.Utf8].toString
      val toi = userData1.get("TimeOfEvent").asInstanceOf[org.apache.avro.util.Utf8].toString
      val body = userData1.get("Body").asInstanceOf[org.apache.avro.util.Utf8].toString


      //et + ":" + es + ":" + ":" + koi + ":" + body

      val doc:JsonDocument = JsonDocument.create("2222",
        JsonObject.create()
          .put("propertyCode", et)
          .put("arrivalDate", es)
          .put("reservationCode", koi)
          .put("lastUpdateTime",body)
      )
      doc
    }
  }
  )

  ds1.printSchema()

  /*val sparkCB = SparkSession
    .builder()
    .appName("N1QLExample")
    .master("local[*]") // use the JVM as the master, great for testing
    .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
    .config("spark.couchbase.bucket.sample-bucket","") // open the travel-sample bucket with empty password
    .config("com.couchbase.username", "Administrator")
    .config("com.couchbase.password", "Welcome123")
    .getOrCreate()

  val sc = sparkCB.sparkContext

  sc.parallelize(Seq(ds1)).saveToCouchbase()*/

  //val JsonDoc = ds1.

/*  val dsJSDoc = ds1.map(
    row => {
      val doc = JsonDocument
        .create("mydoc",
          JsonObject.create()
            .put("type", "")
            .put("reservationStatus", ""))
      doc
    }
  )
  val sc = spark.sparkContext

  sc.parallelize(dsJSDoc).saveToCouchbase()*/


  val query = ds1.writeStream
    .outputMode("append")
    .queryName("table")
    .format("console")
    .start()

  query.awaitTermination()

  spark.stop()
}
