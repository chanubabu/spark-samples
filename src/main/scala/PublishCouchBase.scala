import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object PublishCouchBase extends App{

  // The SparkSession is the main entry point into spark
  val spark = SparkSession
    .builder()
    .appName("N1QLExample")
    .master("local[*]") // use the JVM as the master, great for testing
    .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
    .config("spark.couchbase.bucket.sample-bucket","") // open the travel-sample bucket with empty password
    .config("com.couchbase.username", "Administrator")
    .config("com.couchbase.password", "Welcome123")
    .getOrCreate()

  // Very simple schema, feel free to add more properties here. Properties that do not
  // exist in a streamed document show as null.
  val schema = StructType(
      StructField("META_ID", StringType) ::
      StructField("type", StringType) ::
      StructField("name", StringType) :: Nil
  )

  val lines = spark.read.json("/work/NiFi/sample.json")

 //val jsonLines = {"type":"RESERVATIONS","lastUpdateTime":"12/20/2016 6:44:34 AM","reservationStatus":"Committed","operationProfile":{"primaryLanguage":"en","secondaryLanguage":"en"},"reservation":{"creationDateTime":"12/20/2016 6:44:00 AM","creatorId":"WWWMW","purgeDate":"2017-02-05","reservationConfirmations":{"reservationTypeCode":"14","idContext":"Mariott","reservationCode":"9292327211","arrivalDate":"03022017","reservationInstace":"0"}},"property":{"brandCode":"FI","brandName":"fairFieldIn","propertyCode":"SLCFD","propertyCodeContext":"Mariott","propertyName":"FLS SALT LAKE DTWN","propertyMessages":{"otaInfoType":"18.INF","subSection":{"messageType":"propertyAlert","paragraph":{"messageLanguage":"en-US","messageText":"All rates at this hotel include complimentary in-room high speed Internet access"}}}}}

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
