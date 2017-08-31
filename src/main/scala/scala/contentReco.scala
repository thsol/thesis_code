package scala

import rdd._
import java.io._
import com.amazonaws.ClientConfiguration
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import scala.contentRecoGraph.SimpleCSVHeader


object contentReco {

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

    println("start")

    // declare files and headings
    //val events = sc.textFile(addPrefix("thsolodata/events_test.csv", args))
    val events = sc.textFile(addPrefix("thsolodata/events.csv", args))
    //val events = sc.textFile("/Users/tiegsti.solomon/downloads/events4.csv")
    val page_views = sc.textFile(addPrefix("thsolodata/page_views_sample.csv", args))
    //val page_views = sc.textFile(addPrefix("thsolodata/page_views_sample.csv", args))
    //val page_views = sc.textFile("/Users/tiegsti.solomon/downloads/page_views_sample_test.csv")
    val promoted_content = sc.textFile(addPrefix("thsolodata/promoted_content.csv", args))
    //val promoted_content = sc.textFile(addPrefix("thsolodata/promoted_content.csv", args))
    //val promoted_content = sc.textFile("/Users/tiegsti.solomon/downloads/promoted_content_test.csv")
    val clicks_train = sc.textFile(addPrefix("thsolodata/clicks_train.csv", args))
    //val clicks_train = sc.textFile(addPrefix("thsolodata/clicks_train.csv", args))
    //val clicks_train = sc.textFile("/Users/tiegsti.solomon/downloads/clicks_train_test.csv")

    val page_views_header = mapToHeader(sc, Array("uuid", "document_id", "timestamp", "platform", "geo_location", "traffic_source"))
    val events_header = mapToHeader(sc, Array("display_id", "uuid", "document_id", "timestamp", "platform", "geo_location"))
    val promoted_content_header = mapToHeader(sc, Array("ad_id", "document_id", "campaign_id", "advertiser_id"))
    val clicks_train_header = mapToHeader(sc, Array("display_id", "ad_id", "clicked"))

    val events_headers = events.first
    val page_views_headers = page_views.first
    val promoted_content_headers = promoted_content.first
    val clicks_train_headers = clicks_train.first


    val events_mapped =  events.filter(_(0) != events_headers(0))
      .map(line =>(
        events_header(line.split(",").map(elem => elem.trim), "display_id"),
        events_header(line.split(",").map(elem => elem.trim), "uuid"),
        events_header(line.split(",").map(elem => elem.trim), "document_id")
      )
      )

    val page_views_mapped = page_views.filter(_(0) != page_views_headers(0))
      .map(line =>(
        page_views_header(line.split(",").map(elem => elem.trim), "uuid"),
        page_views_header(line.split(",").map(elem => elem.trim), "document_id"),
        page_views_header(line.split(",").map(elem => elem.trim), "timestamp"),
        page_views_header(line.split(",").map(elem => elem.trim), "platform"),
        page_views_header(line.split(",").map(elem => elem.split(">")(0).trim), "geo_location"),
        page_views_header(line.split(",").map(elem => elem.trim), "traffic_source")
        )
      )

    val promoted_content_mapped =  promoted_content.filter(_(0) != promoted_content_headers(0))
      .map(line =>(
        promoted_content_header(line.split(",").map(elem => elem.trim), "ad_id"),
        promoted_content_header(line.split(",").map(elem => elem.trim), "document_id"),
        promoted_content_header(line.split(",").map(elem => elem.trim), "campaign_id"),
        promoted_content_header(line.split(",").map(elem => elem.trim), "advertiser_id")
      ))

    val clicks_train_mapped =  clicks_train.filter(_(0) != clicks_train_headers(0))
      .map(line =>(
        clicks_train_header(line.split(",").map(elem => elem.trim), "display_id"),
        clicks_train_header(line.split(",").map(elem => elem.trim), "ad_id"),
        clicks_train_header(line.split(",").map(elem => elem.trim), "clicked")
      ))

    val events_mapped_a = events_mapped.map { case (a, b, c) => ((b, c), (a))}
    val page_views_mapped_a = page_views_mapped.map { case (a, b, c, d, e, f) => ((a, b), (c, d, e, f))}
    val clicks_train_mapped_a = clicks_train_mapped.map { case (a, b, c) => (a, (b, c))}
    val promoted_content_mapped_a = promoted_content_mapped.map { case (a, b, c, d) => (a, (b, c, d))}


    val joined1 = page_views_mapped_a
      .join(events_mapped_a)
      .map { case ((uuid, document_id), ((timestamp1, platform1, geo_location1, traffic_source), (display_id)))
      => ((display_id), (document_id, uuid, timestamp1, platform1, geo_location1, traffic_source))}
     // .map {case () }

    joined1.take(5).foreach(println)

    val joined2 = joined1
      .join(clicks_train_mapped_a)
      .map { case ((display_id), ((document_id, uuid, timestamp1, platform1, geo_location1, traffic_source),(ad_id, clicked)))
      => ((ad_id), (document_id, uuid, timestamp1, platform1, geo_location1, traffic_source, clicked, display_id)) } // do filter for no. of clicks here??

    val joined3 = joined2
      .join(promoted_content_mapped_a)
      .map { case ((ad_id), ((document_id, uuid, timestamp1, platform1, geo_location1, traffic_source, clicked, display_id), (document_id2, campaign_id, advertiser_id)))
      =>  (uuid, document_id, ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id) }

    // joined3.take(5).foreach(println)

    val add_page_views = joined3
      .map (r => (((r._1, r._2),1), (r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11)))

    val add_ad_views = joined3
      .map (r => (((r._1, r._3),1), (r._2, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11)))

    val count_clicks = joined3
      .map (r => (((r._1, r._3),1), (r._2, r._4, r._5, r._6, r._7, r._8, r._9, r._10, r._11)))

    val add_ad_views2 = joined3
      .map { case  (uuid, document_id, ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)
      =>  ((uuid, ad_id), (document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)) }

    val add_page_views2 = joined3
      .map { case  (uuid, document_id, ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)
      =>  ((uuid, document_id), (ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)) }

    val count_clicks2 = joined3
      .map { case  (uuid, document_id, ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)
      =>  ((uuid, ad_id), (document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)) }


    val reducedRDD_pv = add_page_views
      .map{case(key, value) => (key._1, key._2)}
      .reduceByKey(_ + _)
      .map{case ((string1, string2), int1) => ((string1, string2),(int1)) }

    val reducedRDD_ad_id_v = add_ad_views
      .map{case(key, value) => (key._1, key._2)}
      .reduceByKey(_ + _)
      .map{case ((string1, string2), int1) => ((string1, string2),(int1)) }

    val reducedRDD_count_clicks = count_clicks
      .map{case(key, value) => (key._1, key._2)}
      .reduceByKey(_ + _)
      .map{case ((string1, string2), int1) => ((string1, string2),(int1)) }


    // reducedRDD_count_clicks.take(5).foreach(println)


    val pv_joined = reducedRDD_pv.join(add_page_views2)
      .map{case ((uuid, document_id), ((int1), (ad_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)))
      => ((uuid, document_id, ad_id), (timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id, int1)) }

    val ad_id_pv_joined = reducedRDD_ad_id_v.join(add_ad_views2)
      .map{case ((uuid, ad_id), ((int1), (document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)))
      => ((uuid, document_id, ad_id), (timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id, int1)) }

    val count_clicks_joined = reducedRDD_count_clicks.join(count_clicks2)
      .map{case ((uuid, ad_id), ((int1), (document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id)))
      => ((uuid, document_id, ad_id), (timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id, int1)) }

    val ad_pv_joined = pv_joined.join(ad_id_pv_joined).map{ case ((uuid, document_id, ad_id),((timestamp1, platform1, geo_location1, traffic_source1, display_id1, clicked1, campaign_id1, advertiser_id1, int1), (timestamp2, platform2, geo_location2, traffic_source2, display_id2, clicked2, campaign_id2, advertiser_id2, int2)))
      =>  ((uuid, document_id, ad_id), (timestamp1, platform1, geo_location1, traffic_source1, display_id1, clicked1, campaign_id1, advertiser_id1, int1, int2))
    }

    val ad_pv_joined2 = ad_pv_joined.join(count_clicks_joined).map {case ((uuid, document_id, ad_id), ((timestamp1, platform1, geo_location1, traffic_source1, display_id1, clicked1, campaign_id1, advertiser_id1, int1, int2), (timestamp2, platform2, geo_location2, traffic_source2, display_id2, clicked2, campaign_id2, advertiser_id2, int3)))
    => (uuid, document_id, ad_id, timestamp1, platform1, geo_location1, traffic_source1, display_id1, clicked1, campaign_id1, advertiser_id1, int1, int2, if (clicked1.equals("0")) 0 else int3 )}
    //.map(line => ((line._1, line._2), _)


    // page view ctr and ad id ctr calculations
    //    val pv_ad_id_CTR = ad_pv_joined
//      .map { case (uuid, ad_id, document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id, pv_count, ad_id_view_count, no_of_clicks)
//      => (uuid, ad_id, document_id, timestamp1, platform1, geo_location1, traffic_source, display_id, clicked, campaign_id, advertiser_id, pv_count,ad_id_view_count, no_of_clicks, no_of_clicks / pv_count, no_of_clicks / ad_id_view_count) }


    // feature output
    val outputfileName_lpids = "content_reco_features.csv"
    //val output_lpids = "/Users/tiegsti.solomon/downloads/" + outputfileName_lpids
    val output_lpids = addPrefix("thsolodata/output/" + outputfileName_lpids, args)
    val header = sc.parallelize("".split(" "))
    ad_pv_joined2.map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5 + "," + x._6 + "," + x._7 + "," + x._8 + "," + x._9 + "," + x._10 + "," + x._11 + "," + x._12 + "," + x._13 + "," + x._14)
      .saveAsMergedCsv(output_lpids, header, false)


  }



  private def mapToHeader(sc: SparkContext, file: String): SimpleCSVHeader = {
    var audience_header_data = sc.textFile(file).map(line => line.split(",").map(elem => elem.trim))
    new SimpleCSVHeader(audience_header_data.take(1)(0))
  }

  private def mapToHeader(sc: SparkContext, header: Array[String]): SimpleCSVHeader = {
    new SimpleCSVHeader(header)
  }

  private def addPrefix(file: String, args: Array[String]): String = {
    if (args(0).equals("local")) {
      "s3n://" + args(1) + ":" + args(2) + "@" + file
    } else {
      "s3n://" + file
    }
  }
}