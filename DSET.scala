package scala

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DSET {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._



    // For implicit conversions like converting RDDs to DataFrames


    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val textFile = sc.textFile(args(0))
    val trdd = textFile.map(x=> (x.split(",")(0),x.split(",")(1)))
    val n = trdd.toDF("follower","following")
    val c = n.groupBy("following").sum()
    logger.warn(c.first()+"nsdckjsnclkmscklmwscsmclkmsklvmlksfmmvsvkdnslv slclkskl")
    /*val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .aggregateByKey(0)((n: Int, v: Int) => n + 1,(p1: Int, p2: Int) => p1 + p2)
    counts.saveAsTextFile(args(1))*/
    logger.warn(c.explain())
    c.rdd.saveAsTextFile(args(1))

  }

}
