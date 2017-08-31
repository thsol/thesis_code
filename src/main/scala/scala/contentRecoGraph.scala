package scala

import java.io._

import com.amazonaws.ClientConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{GetObjectRequest, PutObjectRequest}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, Days}

import scala.contentReco.addPrefix


object contentRecoGraph {

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

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.Column

    val spark = SparkSession
      .builder
      .getOrCreate()

    val outputfileName_lpids = "content_reco_features"
    val output_lpids = addPrefix("thsolodata/output/" + outputfileName_lpids, args)

    // output as input
    val promoted_content_DF = spark.read.option("header", "true").csv(output_lpids).cache()

    // create nodes.csv file, output to S3
    var nodes_DF = 0// take from csv uuid, ad_id, doc_id, topic_id DF, distinct uuid!

    // .. then remove uuid column we no longer need that
    // output the file to graph folder thsolodata/graph folder

    // save node
    //    nodes_DF
    //      .coalesce(1)
    //      .write.format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save(nodes)

    var edges_DF = 0 // group all distinct UUIDS, match all adids together

    var replace_doc_topic = 0 //find all docids for LEFT adid

    // create edges.csv file, output to S3

    // save edges
    //    edges_DF
    //      .coalesce(1)
    //      .write.format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save(edges)


  }
  private def createDateRange(file: String, start: DateTime, end: DateTime, suffix: String = ""): IndexedSeq[String] = {
    val numberOfDays = Days.daysBetween(start, end).getDays()
    for (f <- 0 to numberOfDays) yield file + start.plusDays(f).toString("yyyyMMdd") + suffix
  }

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

  class SimpleCSVHeader(header: Array[String]) extends Serializable {
    val index = header.zipWithIndex.toMap

    def apply(array: Array[String], key: String): String = array(index(key))
  }

}