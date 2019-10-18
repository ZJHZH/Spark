package day02.LoginLog

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * date 2019-10-09 19:05<br>
 *
 * @author ZJHZH
 */
object LoginLog {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LoginLog").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val lac_info: RDD[(String, (String, String))] = sc.textFile(args(0) + "lac_info.txt").map(e => {
      val strings: Array[String] = e.split("[^\\w.]+")
      (strings(0), (strings(1), strings(2)))
    })
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sc.textFile(args(0) + "log/").map(e => {
      val strings: Array[String] = e.split("[^\\w]+")
      ((strings(0), strings(2)), (strings(1), strings(3)))
    }).groupByKey().map(e => {
      val tuples: mutable.Buffer[(Long, String)] = e._2.toBuffer.sortBy((e1: (String, String)) => {
        e1._1
      }).map(e2 => {
        val time: String = e2._1
        (format.parse(time).getTime / 1000, e2._2)
      })
      if (tuples.head._2 == "0") {
        tuples.remove(0)
      }
      if (tuples.last._2 == "1") {
        tuples.remove(tuples.length - 1)
      }
      val login: mutable.Buffer[Long] = tuples.filter(_._2 == "1").map(_._1)
      val logout: mutable.Buffer[Long] = tuples.filter(_._2 == "0").map(_._1)
      val sum: Long = login.zip(logout).map((e3: (Long, Long)) => e3._2 - e3._1).sum
      (e._1, sum)
    }).map(e => {
      (e._1._2, (e._1._1, e._2))
    }).join(lac_info).map(e => {
      val value: ((String, Long), (String, String)) = e._2
      (value._1._1, (e._1, value._1._2, value._2._1, value._2._2))
    }).groupByKey().map(e => {
      val value: Iterable[(String, Long, String, String)] = e._2
      (e._1, value.toBuffer.sortBy((_: (String, Long, String, String))._2).takeRight(2).reverse)
    }).saveAsTextFile(args(1))

    sc.stop()
  }















  def test2(): Unit ={
    val conf: SparkConf = new SparkConf().setAppName("LoginLog").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val lac_info: RDD[(String, (String, String))] = sc.textFile("hdfs://hadoop04:9000/data/input/lacduration/" + "lac_info.txt").map(e => {
      val strings: Array[String] = e.split("[^\\w.]+")
      (strings(0), (strings(1), strings(2)))
    })
    val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    sc.textFile("hdfs://hadoop04:9000/data/input/lacduration/" + "log/").map(e => {
      val strings: Array[String] = e.split("[^\\w]+")
      ((strings(0), strings(2)), (strings(1), strings(3)))
    }).groupByKey().map(e => {
      val tuples: mutable.Buffer[(Long, String)] = e._2.toBuffer.sortBy((e1: (String, String)) => {
        e1._1
      }).map(e2 => {
        val time: String = e2._1
        (format.parse(time).getTime / 1000, e2._2)
      })
      if (tuples.head._2 == "0") {
        tuples.remove(0)
      }
      if (tuples.last._2 == "1") {
        tuples.remove(tuples.length - 1)
      }
      val login: mutable.Buffer[Long] = tuples.filter(_._2 == "1").map(_._1)
      val logout: mutable.Buffer[Long] = tuples.filter(_._2 == "0").map(_._1)
      val sum: Long = login.zip(logout).map(e3 => e3._2 - e3._1).sum
      (e._1, sum)
    }).map(e => {
      (e._1._2, (e._1._1, e._2))
    }).join(lac_info).map(e => {
      val value: ((String, Long), (String, String)) = e._2
      (value._1._1, (e._1, value._1._2, value._2._1, value._2._2))
    }).groupByKey().map(e => {
      val value: Iterable[(String, Long, String, String)] = e._2
      (e._1, value.toBuffer.sortBy((_: (String, Long, String, String))._2).takeRight(2).reverse)
    }).sortByKey().saveAsTextFile("hdfs://hadoop04:9000/out/loginLog/01")
  }

  def test1(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LoginLog").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val lac_info: RDD[(String, (String, String))] = sc.textFile(args(0) + "lac_info.txt").map(e => {
      val strings: Array[String] = e.split("[^\\w.]+")
      (strings(0), (strings(1), strings(2)))
    })
    val log: RDD[String] = sc.textFile(args(0) + "log/")
    log.map(e => {
      val strings: Array[String] = e.split("[^\\w]+")
      ((strings(0), strings(2)), (strings(1), strings(3)))
    }).groupByKey().map(e => {
      val tuples: mutable.Buffer[(Long, String)] = e._2.toBuffer.sortBy((e1: (String, String)) => {
        e1._1
      }).map(e2 => {
        val time: String = e2._1
        (new SimpleDateFormat("yyyyMMddHHmmss").parse(time).getTime, e2._2)
      })
      if (tuples.head._2 == "0") {
        tuples.remove(0)
      }
      if (tuples.last._2 == "1") {
        tuples.remove(tuples.length - 1)
      }
      val login: mutable.Buffer[Long] = tuples.filter(_._2 == "1").map(_._1)
      val logout: mutable.Buffer[Long] = tuples.filter(_._2 == "0").map(_._1)
      val sum: Long = login.zip(logout).map(e3 => e3._2 - e3._1).sum
      (e._1, sum)
    }).map(e => {
      (e._1._2, (e._1._1, e._2))
    }).join(lac_info).map(e => {
      val value: ((String, Long), (String, String)) = e._2
      (e._1, (value._1._1, value._1._2, value._2._1, value._2._2))
    }).groupByKey().map(e => {
      val value: Iterable[(String, Long, String, String)] = e._2
      (e._1, value.toBuffer.sortBy((_: (String, Long, String, String))._2).takeRight(2).reverse)
    }).sortByKey().saveAsTextFile(args(1))
  }
}
