package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * date 2019-10-08 20:50<br>
 *
 * @author ZJHZH
 */
object Exercise3 {
  def main(args: Array[String]): Unit = {
    val context: SparkContext = new SparkContext(new SparkConf().setAppName("Exercise2").setMaster("local"))
    val rdd1: RDD[String] = context.textFile(args(0))
    val rdd2: RDD[Advert] = rdd1.map(e => {
      val strings: mutable.ArrayOps[String] = e.split(" ")
      val timestamp: Long = strings(0).toLong
      val province: Int = strings(1).toInt
      val city: Int = strings(2).toInt
      val userid: Int = strings(3).toInt
      val adid: Int = strings(4).toInt
      new Advert(timestamp, province, city, userid, adid)
    })
    rdd2.filter(_.istrue()).map(e=>{
      ((e.province, e.adid),1)
    }).reduceByKey(_ + _).map(e => {
      val value: (Int, Int) = e._1
      (value._1, value._2, e._2)
    }).groupBy(_._1).map((e: (Int, Iterable[(Int, Int, Int)])) =>{
      val sorted: List[Int] = e._2.toList.sortBy(_._3).takeRight(3).map(_._2).sorted
      (e._1,sorted)
    }).sortByKey().saveAsTextFile(args(1))
    context.stop()
  }
}
class Advert(val timestamp:Long,val province:Int,val city:Int,val userid:Int,val adid:Int){
  def istrue(): Boolean ={
    if (userid < 0 || userid > 99) return false
    if (province != city || province < 0 || province > 9) return false
    if (adid < 0 || adid > 19) return false
    return true
  }
}