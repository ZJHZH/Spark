package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-08 19:55<br>
 *
 * @author ZJHZH
 */
object Exercise1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Exercise1").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = context.makeRDD(Array(("aa",3),("cc",6),("bb",2),("dd",1)))
    val rdd2: RDD[(String, Int)] = rdd1.map(e=>(e._2,e._1)).sortByKey().map(e=>(e._2,e._1))
    println(rdd2.collect.toBuffer)
  }
}
