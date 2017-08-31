/**
 * Created by will.hutchinson.
 */
import java.net.URI

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/* Deployment Zone
* @http://deploymentzone.com/2015/01/30/spark-and-merged-csv-files
*
* */

package object rdd {

  implicit class RddOp[P <: Any](val rdd: RDD[P]) {
    def saveAsMergedCsv(path: String, header: RDD[String],  zip: Boolean, overwrite: Boolean = true):
    Unit = {
      val filename = FilenameUtils.getName(path)
      //      val csv: RDD[String] = rdd.map(toCsv(_)) //maybe remove this and just use the rdd in place of csv to get rid of ending new line
      //      val uri: URI = new URI(s"hdfs:///root/ephemeral-hdfs/$filename")
      val uri: URI = new URI(s"$filename")
      if (overwrite) {
        val hdfs: FileSystem = FileSystem.get(uri,
          rdd.context.hadoopConfiguration)
        hdfs.delete(new Path(uri), true)
      }
      //val res = header.union(csv)
      val gzipCodec = new GzipCodec
      gzipCodec.setConf(new Configuration())
      if(zip) rdd.saveAsTextFile(uri.toString, classOf[GzipCodec])
      else rdd.saveAsTextFile(uri.toString)
      merge(rdd.context, uri.toString, path)
    }
  }

  private def merge(sc: SparkContext,
    srcPath: String,
    dstPath: String): Unit = {
    val srcFileSystem = FileSystem.get(new URI(srcPath), sc.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(dstPath), sc.hadoopConfiguration)
    dstFileSystem.delete(new Path(dstPath), true)
    FileUtil.copyMerge(srcFileSystem, new Path(srcPath), dstFileSystem, new Path(dstPath), true, sc.hadoopConfiguration, null)
  }

  private def toCsv(a: Any): String = {
    def toCsv(a: Any, accumulator: Vector[String]):
    Vector[String] = {
      a match {
        // unfortunately null can happen for some results from Jdbc
        case null | None =>
          accumulator
        case s: String =>
          accumulator :+ s
        case p: Product =>
          p.productIterator.map(toCsv(_, accumulator))
            .toVector.flatten
        case arr: Array[_] =>
          arr.map(toCsv(_, accumulator))
            .toVector.flatten
        case _ =>
          accumulator :+ a.toString
      }
    }

    toCsv(a, Vector()).mkString(",")
  }
}

