package scala

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RDDG {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events/")


    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupByKey()
      .map{case(str, nums) => (str, nums.sum)}
    logger.warn("\n hello"+counts.toDebugString+ "kjnsdcjnacdk dcmv amlsv  avf wlv ")
    counts.saveAsTextFile(args(1))
  }

}
