package scala

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object RSJoin {



  def main(args: Array[String]) {
    val log = LogManager.getRootLogger
    //val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      log.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val tf = sc.textFile(args(0))




    val to_list: List[String]=List()
    val from_list: List[String]=List()




    val textFile = tf.filter(line => (line.split(",")(0).toInt<=10000) && (line.split(",")(1).toInt<=10000))
    val from = textFile.flatMap(line1=> {List ((line1.split(",")(0),line1.split(",")(1) ))})
    val to = textFile.flatMap(line2 => {List ((line2.split(",")(1),line2.split(",")(0) ))})
    log.warn(from.first())
    val firstjoin = to.join(from)
    log.warn(firstjoin.first())
    val newjoin =firstjoin.map{case (x,(y,z))=>((z,y ),x)}
    val newleftjoin = textFile.map{case x =>((x.split(",")(0),x.split(",")(1) ), null) }
    val finaljoin = newjoin.join(newleftjoin)
    log.warn(finaljoin.count()/3)



  }
}
