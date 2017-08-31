package scala

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

import scala.contentReco.{addPrefix, mapToHeader}
import scala.contentRecoGraph.SimpleCSVHeader


object logisticRegression {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LogReg")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .getOrCreate()

    // define dataframe schema
    val schema = StructType (
      StructField("ad_id", DoubleType, true) ::
        StructField("document_id", DoubleType, true) ::
        StructField("platform", DoubleType, true) ::
        StructField("geo_location", DoubleType, true) ::
        StructField("traffic_source", DoubleType, true) ::
        StructField("display_id", DoubleType, true) ::
        StructField("clicked", DoubleType, true) ::
        StructField("campaign_id", DoubleType, true) ::
        StructField("advertiser_id", DoubleType, true) ::
        StructField("pv_count", DoubleType, true) ::
        StructField("ad_id_view_count", DoubleType, true) ::
        StructField("no_of_clicks", DoubleType, true) ::
        StructField("pCTR", DoubleType, true) ::
        StructField("adCTR", DoubleType, true) ::  Nil)

    //val training = spark.read.format("csv").load("/Users/tiegsti.solomon/Downloads/content_reco_features_mod.csv")
    val training = sc.textFile("/Users/tiegsti.solomon/Downloads/content_reco_features_mod.csv")

    val features_header = mapToHeader(sc, Array("ad_id","document_id", "platform", "geo_location", "traffic_source","display_id",
      "clicked","campaign_id","advertiser_id","pv_count","ad_id_view_count","no_of_clicks","pCTR","adCTR"))

    val trainingRDD = training
      .map(line =>(Row(
        features_header(line.split(",").map(elem => elem.trim), "ad_id"),
        features_header(line.split(",").map(elem => elem.trim), "document_id"),
        features_header(line.split(",").map(elem => elem.trim), "platform"),
        features_header(line.split(",").map(elem => elem.trim), "geo_location"),
        features_header(line.split(",").map(elem => elem.trim), "traffic_source"),
        features_header(line.split(",").map(elem => elem.trim), "display_id"),
        features_header(line.split(",").map(elem => elem.trim), "clicked"),
        features_header(line.split(",").map(elem => elem.trim), "campaign_id"),
        features_header(line.split(",").map(elem => elem.trim), "advertiser_id"),
        features_header(line.split(",").map(elem => elem.trim), "pv_count"),
        features_header(line.split(",").map(elem => elem.trim), "ad_id_view_count"),
        features_header(line.split(",").map(elem => elem.trim), "no_of_clicks"),
        features_header(line.split(",").map(elem => elem.trim), "pCTR")
      )))

    // categorical feature
    val geo_location_indexer = new StringIndexer().setInputCol("geo_location").setOutputCol("geo_location_category")

    //    //assemble raw feature
//    val assembler = new VectorAssembler()
//      .setInputCols(Array("MonthCat", "DayofMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "OriginCat", "DepTime", "CRSDepTime", "ArrTime", "CRSArrTime", "ActualElapsedTime", "CRSElapsedTime", "AirTime","DepDelay", "Distance"))
//      .setOutputCol("features")

    val trainingRDD_row = trainingRDD
      .map(line => Row((line(0)),(line(1)),(line(2)),(line(3)),(line(4)),(line(5)),(line(6)),
        (line(7)),(line(8)),(line(9)),(line(10)),(line(11)),(line(12))))

    println("-------------------------------------------------------------------------")

    val training_rowDF = spark.createDataFrame(trainingRDD_row, schema)

    training_rowDF.take(5).foreach(println)


    //
//    cleanDF.take(5).foreach(println)

    // feed model
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
//
//    // fit model
//    val lrModel = lr.fit(training)
//
//    // binary classification
//    val mlr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.3)
//      .setElasticNetParam(0.8)
//
//    val mlrModel = mlr.fit(training)

    //    // LR multinomial coefficients and intercepts
    //    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    //    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

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