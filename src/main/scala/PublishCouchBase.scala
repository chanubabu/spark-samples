import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object PublishCouchBase extends App{

  // The SparkSession is the main entry point into spark
  val spark = SparkSession
    .builder()
    .appName("N1QLExample")
    .master("local[*]") // use the JVM as the master, great for testing
    .config("spark.couchbase.nodes", "localhost") // connect to couchbase on localhost
    .config("spark.couchbase.bucket.sample-bucket","") // open the travel-sample bucket with empty password
    .getOrCreate()

  // Very simple schema, feel free to add more properties here. Properties that do not
  // exist in a streamed document show as null.
  val schema = StructType(
    StructField("META_ID", StringType) ::
      StructField("type", StringType) ::
      StructField("name", StringType) :: Nil
  )

  // Define the Structured Stream from Couchbase with the given Schema
  val records = spark.readStream
    .format("com.couchbase.spark.sql")
    .schema(schema)
    .load()

  // Count per type and print to screen
  records
    .groupBy("type")
    .count()
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()

  spark.stop()
}
