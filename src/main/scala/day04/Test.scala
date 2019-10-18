package day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * date 2019-10-12 12:08<br>
 *
 * @author ZJHZH
 */
object Test {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
    val map: MyAccumulatorMap = new MyAccumulatorMap
    sc.register(map)
    sc.textFile("E:\\TEST\\Input\\subjectaccess\\").flatMap(_.split("[^\\w]+")).foreach(map.add)
    println(map.value.toBuffer.sortBy((_: (String, Int))._2).reverse)
  }
  def test1(): Unit ={
    val map1: mutable.HashMap[String, Int] = scala.collection.mutable.HashMap[String, Int]()
    map1("a") = 3
    map1("c") = 4
    val map2: mutable.HashMap[String, Int] = mutable.HashMap[String,Int]()
    map2("a") = 7
    map2("b") = 2

    map2.foreach(e=>{
      if (map1.contains(e._1)){
        map1(e._1) += e._2
      }else{
        map1(e._1) = e._2
      }
    })
    println(map1)
  }
}
