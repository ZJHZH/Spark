package day03

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * date 2019-10-11 19:28<br>
 *
 * @author ZJHZH
 */
object CustomSortDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dogInfo: RDD[(String, Int, Int)] = sc.parallelize(List(("xiaoming", 90, 32), ("xiaohong", 70, 32), ("xiaobai", 80, 34), ("haige", 90, 35)))
    val DogRDD: RDD[Dog2] = dogInfo.map(tup => {
      Dog2(tup._1, tup._2, tup._3)
    })
    val sorted: RDD[Dog2] = DogRDD.sortBy(dog => dog)
    println(sorted.collect.toBuffer)
  }
}

case class Dog2(name: String, faceValue: Int, age: Int) extends Ordered[Dog2] {
  override def compare(that: Dog2): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    } else {
      this.faceValue - that.faceValue
    }
  }

  override def toString: String = "name:" + name + " faceValue:" + faceValue + " age:" + age
}
