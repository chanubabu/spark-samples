import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object JsonParser extends App {

  val IP = "localhost"
  val TOPIC = "test1"

  val spark = SparkSession
    .builder()
    .appName("kafka-consumer")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val ds1 = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", IP + ":9092")
    .option("zookeeper.connect", IP + ":2181")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("max.poll.records", 10)
    .option("failOnDataLoss", false)
    .load()

  //{"id":12324,"name":"user"}
  //id - LongType
  //name - StringType
  /*val schema = StructType(Seq(
    StructField("date", LongType, true),  //yr-day-hh-propcode
    StructField("code", StringType, true)
  ))*/

  val schema = StructType(Seq(
    StructField("partKey", LongType, true)  //yr-day-hh-propcode
  ))


  val df = ds1.selectExpr("cast (value as string) as json")
              .select(from_json($"json", schema=schema)
              .as("data")).select("data.*")

  println(df.isStreaming)

  val query = df.writeStream
    .outputMode("append")
    .queryName("table")
    .format("console")
    .start()

  query.awaitTermination()

  //spark.sql("select * from table").show(truncate = false)
  //val x = spark.sql("select * from table")

  spark.stop()

}
