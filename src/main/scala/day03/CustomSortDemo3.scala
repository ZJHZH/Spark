package day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * date 2019-10-11 19:30<br>
 *
 * @author ZJHZH
 */
object CustomSortDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dogInfo: RDD[(String, Int, Int)] = sc.parallelize(List(("xiaoming", 90, 32), ("xiaohong", 70, 32), ("xiaobai", 80, 34), ("haige", 90, 35)))
    val DogRDD: RDD[Dog3] = dogInfo.map(tup => {
      Dog3(tup._1, tup._2, tup._3)
    })
    implicit val Ordering = new Ordering[Dog3]{
      override def compare(x: Dog3, y: Dog3): Int = {
        if (x.faceValue == y.faceValue) {
          x.age - y.age
        } else {
          y.faceValue - x.faceValue
        }
      }
    }
    val sorted: RDD[Dog3] = DogRDD.sortBy(dog => dog)
    println(sorted.collect.toBuffer)
  }

}

case class Dog3(name: String, faceValue: Int, age: Int)
