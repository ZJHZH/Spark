package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
 * date 2019-10-11 20:21<br>
 *
 * @author ZJHZH
 */
object Exercise1 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Exercise1").setMaster("local[*]"))
    val rdd: RDD[(String, Iterable[String])] = sc.textFile(args(0)).map(e => {
      val subject: String = e.replaceFirst(".*? http://([\\w]+)\\.learn\\.com/.*", "$1")
      (subject, e)
    }).groupByKey()
    val newnumPartitions: Int = rdd.count().toInt
    val map: Map[String, Int] = rdd.keys.collect().zipWithIndex.toMap

    rdd.partitionBy(new Partitioner {
      override def numPartitions: Int = newnumPartitions

      override def getPartition(key: Any): Int = {
        map(key.toString)
      }
    }).mapPartitions(_.flatMap(_._2)).saveAsTextFile(args(1))

    sc.stop()
  }
}
