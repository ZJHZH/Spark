package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * date 2019-10-08 20:00<br>
 *
 * @author ZJHZH
 */
object Exercise2 {
  def main(args: Array[String]): Unit = {
    val context: SparkContext = new SparkContext(new SparkConf().setAppName("Exercise2").setMaster("local"))
    context.textFile(args(0)).map(_.split(" ")).filter((e: Array[String]) => {
      val province: Int = e(1).toInt
      val city: Int = e(2).toInt
      val userid: Int = e(3).toInt
      val adid: Int = e(4).toInt
      if (userid < 0 || userid > 99) false
      else if (province == city || province < 0 || province > 9) false
      else if (adid < 0 || adid > 19) false
      else true
    }).map(e => {
      ((e(1).toInt, e(4).toInt), 1)
    }).reduceByKey(_ + _).map(e => {
      val value: (Int, Int) = e._1
      (value._1, value._2, e._2)
    }).groupBy(_._1).map((e: (Int, Iterable[(Int, Int, Int)])) => {
      val sorted: List[Int] = e._2.toList.sortBy(_._3).takeRight(3).map(_._2).sorted
      (e._1, sorted)
    }).sortByKey().saveAsTextFile(args(1))
    context.stop()
  }
}