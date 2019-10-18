package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * date 2019-10-08 21:55<br>
 *
 * @author ZJHZH
 */
object Test {
  def main(args: Array[String]): Unit = {

  }
  def test2(): Unit ={
    val context: SparkContext = new SparkContext(new SparkConf().setAppName("Exercise2").setMaster("local"))
    //    val rdd1: RDD[String] = context.textFile(args(0))
    val rdd2: RDD[(String, Int)] = context.parallelize(List(("aa",1),("bb",1),("aa",1),("bb",1),("aa",1),("bb",1),("aa",1),("bb",1)),2)
    println(rdd2.mapPartitionsWithIndex((i, it) => {
      it.map(e => {
        (i,e)
      })
    }).collect().toBuffer)
    val rdd3: RDD[(String, Int)] = rdd2.aggregateByKey(10)(_+_,_+_)
    println(rdd3.collect().toBuffer)
  }
  def test1(): Unit ={
    val bool: Boolean = (1,2) == (2,1)
    println(bool)
  }
}
