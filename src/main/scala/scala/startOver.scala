
import java.io._
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDate

import com.amazonaws.ClientConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectRequest}
import com.github.nscala_time.time.Imports.DateTime
import org.apache.log4j.{Level, Logger}
import org.joda.time.{DateTime, Days}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{concat, count, lit}
import org.apache.spark.sql.functions.split
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.types.{IntegerType, TimestampType}
import org.joda.time.format.DateTimeFormat

import scala.contentReco.addPrefix
import scala.contentRecoGraph.SimpleCSVHeader

object startOver {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val client = new ClientConfiguration
    client.setSocketTimeout(300000)
    client.setConnectionTimeout(300000)
    val credentials = new BasicAWSCredentials(args(1), args(2))
    val amazonS3Client = new AmazonS3Client(credentials, client)

//    import org.apache.spark.sql.SparkSession
//
//    val spark = SparkSession
//      .builder
//      .getOrCreate()

    print("start")

//    // declare files and headings
//    val events = sc.textFile(addPrefix("thsolodata/events_test.csv", args))
//    val page_views = sc.textFile(addPrefix("thsolodata/page_views_sample.csv", args))
//    val promoted_content = sc.textFile(addPrefix("thsolodata/promoted_content_test.csv", args))
//    val clicks_train = sc.textFile(addPrefix("thsolodata/clicks_train_test.csv", args))
//
//    val page_views_header = mapToHeader(sc, Array("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source"))
//    val events_header = mapToHeader(sc, Array("display_id", "uuid", "document_id", "timestamp", "platform", "geo_location"))
//    val promoted_content_header = mapToHeader(sc, Array("ad_id", "document_id", "campaign_id", "advertiser_id"))
//    val clicks_train_header = mapToHeader(sc, Array("display_id", "ad_id", "clicked"))
//
//    val events_mapped =  events
//    .map(line =>(
//        events_header(line.split(",").map(elem => elem.trim), "display_id"),
//      (
//        events_header(line.split(",").map(elem => elem.trim), "uuid"),
//        events_header(line.split(",").map(elem => elem.trim), "document_id")
//      ),
//        events_header(line.split(",").map(elem => elem.trim), "timestamp"),
//        events_header(line.split(",").map(elem => elem.trim), "platform"),
//      events_header(line.split(",").map(elem => elem.trim), "geo_location")
//      )
//    )
//
//    val page_views_mapped = page_views
//      .map(line =>((
//        page_views_header(line.split(",").map(elem => elem.trim), "uuid"),
//        page_views_header(line.split(",").map(elem => elem.trim), "document_id")
//      ),
//        page_views_header(line.split(",").map(elem => elem.trim), "timestamp"),
//        page_views_header(line.split(",").map(elem => elem.trim), "platform"),
//        page_views_header(line.split(",").map(elem => elem.trim), "geo_location"),
//        page_views_header(line.split(",").map(elem => elem.trim), "traffic_source")
//        )
//      )
//
//    val promoted_content_mapped =  promoted_content
//      .map(line =>(
//        promoted_content_header(line.split(",").map(elem => elem.trim), "ad_id"),
//        promoted_content_header(line.split(",").map(elem => elem.trim), "document_id"),
//        promoted_content_header(line.split(",").map(elem => elem.trim), "campaign_id"),
//        promoted_content_header(line.split(",").map(elem => elem.trim), "advertiser_id")
//      ))
//
//    val clicks_train_mapped =  clicks_train
//      .map(line =>(
//        clicks_train_header(line.split(",").map(elem => elem.trim), "display_id"),
//        clicks_train_header(line.split(",").map(elem => elem.trim), "ad_id"),
//        clicks_train_header(line.split(",").map(elem => elem.trim), "clicked")
//      ))
//
//    val events_mapped_a = events_mapped.map { case (a, (b,c), d, e, f) => (b, (a, c, d, e, f))}
//    val page_views_mapped_a = page_views_mapped.map { case ((a,b), c, d, e, f) => (b, (a, c, d, e, f))}
//
//    val events_page_views_joined1 = events_mapped_a
//      .join(page_views_mapped_a)
//      .map { case (uuid1, ((document_id2, timestamp2, platform2, geo_location2, traffic_source), (display_id, document_id1, timestamp1, platform1, geo_location1)))
//      => (uuid1, (document_id2, timestamp2, platform2, geo_location2, traffic_source, display_id))}
//     // .map {case () }
//
//  events_page_views_joined1.take(5).foreach(println)

  }

  //    // declare files and headings
  //    val events_DF = spark.read.option("header", "true").csv(addPrefix("thsolodata/events.csv",args))
  //    val page_views_DF = spark.read.option("header", "true").csv(addPrefix("thsolodata/page_views_sample.csv",args))
  //    val promoted_content_DF = spark.read.option("header", "true").csv(addPrefix("thsolodata/promoted_content.csv",args))
  //    val clicks_train_DF = spark.read.option("header", "true").csv(addPrefix("thsolodata/clicks_train.csv",args))
  //
  //
  //    var features1 = events_DF.join(page_views_DF,
  //        events_DF("document_id") === page_views_DF("document_id") and events_DF("uuid") === page_views_DF("uuid"))
  //      .drop(page_views_DF("uuid")).drop(page_views_DF("timestamp")).drop(page_views_DF("platform")).drop(page_views_DF("geo_location"))
  //      .drop(page_views_DF("document_id"))
  //
  //    var features2 = features1
  //      .join(clicks_train_DF, features1("display_id") === clicks_train_DF("display_id")).drop(clicks_train_DF("display_id"))
  //
  //    var features3 = features2
  //      .join(promoted_content_DF, features2("ad_id") === promoted_content_DF("ad_id")).drop(promoted_content_DF("document_id")).drop(promoted_content_DF("ad_id"))
  //
  //    // page view feature
  //    var features4 = features3
  //      .join(features3.groupBy("document_id","uuid").count(), "document_id")
  //      .withColumnRenamed("count", "page_views").drop(features3("uuid"))//.drop(features3("document_id"))
  //
  //    // count distinct user clicks
  //    var features5 = features4
  //      .join(features4.groupBy("ad_id","uuid").count(), "uuid")
  //      .filter(features4("clicked")==="1")
  //      .withColumnRenamed("count", "no_user_clicks").drop(features4("ad_id"))
  //
  ////    // ad id view feature
  //    var features6 = features5
  //      .join(features5.groupBy("ad_id","uuid").count(), "ad_id")//.drop(features4("ad_id"))
  //      .withColumnRenamed("count", "ad_id_views").drop(features5("uuid"))
  //
  //    // adid CTR .. output in dec
  //    var features7 = features6
  //       .join(features6.groupBy("ad_id","uuid").agg(count("ad_id").alias("total_ad_view")), "ad_id")
  //       .withColumn("adid_CTR",(features6("clicked")/ features6("ad_id_views"))).drop(features6("uuid"))
  //
  ////    println("---------features7---------")
  ////    features7.take(6).foreach(println)
  //
  //    // page CTR .. output in dec
  //    var features8 = features7
  //      .join(features7.groupBy("document_id","uuid").agg(count("uuid").alias("total_pv")), "document_id")
  //      .withColumn("page_CTR", (features7("clicked") / features7("page_views"))).drop(features7("uuid"))
  //
  ////    println("---------features8---------")
  ////    features8.take(6).foreach(println)
  //
  //    var features9 = features8.withColumn("country", split(features7("geo_location"), ">")(0))
  //      .withColumn("state", split(features8("geo_location"), ">")(1))
  //      .withColumn("dma", split(features8("geo_location"), ">")(2))
  //      .drop(features8("uuid")).drop(features8("geo_location")).drop(features8("timestamp"))
  ////
  ////    println("---------features9---------")
  ////    features9.take(6).foreach(println)
  //
  //
  //    if(features9.select("state").first().isNullAt(0)){
  //      features8 = features8.drop("state");}
  //
  //    if(features9.select("dma").first().isNullAt(0)){
  //      features8 = features8.drop("dma");}
  //
  //////    println("---------features9---------")
  ////    features9.take(6).foreach(println)
  //
  //// page views counts of ad landing page  // impression counts for each ad-id landing doc id, campaign and advertiser_id (if click data merged with promoted_content)
  //val outputfileName_lpids = "content_reco_features"
  //val output_lpids = addPrefix("thsolodata/output/" + outputfileName_lpids, args)


  private def addPrefix(file: String, args: Array[String]): String = {
    // if arguments provided, include them in S3 bucket url
    if (args(0).equals("local")) {
      "s3n://" + args(1) + ":" + args(2) + "@" + file
    } else {
      "s3n://" + file
    }
  }

  private def mapToHeader(sc: SparkContext, file: String): SimpleCSVHeader = {
    var audience_header_data = sc.textFile(file).map(line => line.split(",").map(elem => elem.trim))
    new SimpleCSVHeader(audience_header_data.take(1)(0))
  }

  private def mapToHeader(sc: SparkContext, header: Array[String]): SimpleCSVHeader = {
    new SimpleCSVHeader(header)
  }
}
