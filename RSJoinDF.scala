package scala

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RSJoinDF {

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



    val tf = textFile.filter(line => (line.split(",")(0).toInt<=10000) && (line.split(",")(1).toInt<=10000))
    val trdd = tf.map(x=> (x.split(",")(0),x.split(",")(1)))
    val left = trdd.toDF("follower","following")
    val rightext = tf.map({
      case x => (x.split(",")(1),x.split(",")(0) )
    })
    val right = rightext.toDF("follower", "following")
    val twopath = right.join(left,"follower" )
    twopath.schema.fields.foreach(x => logger.warn(x))
    val finaljoinleft =   rightext.toDF( "following1","following2")
    val newtp = twopath.toDF("follower","following1","following2")
    val finaljoin = newtp.join(finaljoinleft,Seq("following1","following2"))
    logger.warn(finaljoin.count()/3)

    //val df =spark.read(

  }

}
