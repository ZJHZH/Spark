package day03

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, RangePartitioner, SparkConf, SparkContext}



/**
 * date 2019-10-11 14:35<br>
 *
 * @author ZJHZH
 */
object Test {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Exercise1").setMaster("local[*]"))
    sc.setCheckpointDir("E:\\TEST\\check\\01")
    val rdd1: RDD[(String, String)] = sc.textFile("E:\\TEST\\Input\\subjectaccess\\").map(e => {
      val subject: String = e.replaceFirst(".*? http://([\\w]+)\\.learn\\.com/.*", "$1")
      (subject, e)
    })
    rdd1.checkpoint()
    val rdd2: RDD[(String, Iterable[String])] = rdd1.groupByKey()
    rdd2.checkpoint()
    val newnumPartitions: Int = rdd2.count().toInt
    val map: Map[String, Int] = rdd2.keys.collect().zipWithIndex.toMap

    val rdd3: RDD[(String, Iterable[String])] = rdd2.partitionBy(new Partitioner {
      override def numPartitions: Int = newnumPartitions

      override def getPartition(key: Any): Int = {
        map(key.toString)
      }
    })
    rdd3.checkpoint()

    val rdd4: RDD[String] = rdd3.mapPartitions(_.flatMap(_._2))
//    val rdd4: RDD[Char] = rdd3.mapPartitions(_.flatMap(_._1))
    rdd4.checkpoint()

    rdd4.saveAsTextFile("E:\\TEST\\Output\\14")

    sc.stop()
  }
  def test(): Unit ={
    val value: String = "20161123101523 http://java.learn.com/java/javaee.shtml"
//    val host: String = new URL(value).getHost
//    println(host)
    val str: String = value.replaceFirst(".*? http://([\\w]+)\\.learn\\.com/.*","$1")
    println(str)

    val str1: String = "name: %s age: %d".format("xiaoming", 18)
    println(str1)
  }
}
