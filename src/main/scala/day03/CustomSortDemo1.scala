package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-11 19:03<br>
 *
 * @author ZJHZH
 */
object CustomSortDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSortDemo").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dogInfo: RDD[(String, Int, Int)] = sc.parallelize(List(("xiaoming", 90, 32), ("xiaohong", 70, 32), ("xiaobai", 80, 34), ("haige", 90, 35)))
    val DogRDD: RDD[Dog1] = dogInfo.map(tup => {
      new Dog1(tup._1, tup._2, tup._3)
    })
    val sorted: RDD[Dog1] = DogRDD.sortBy(dog => dog)
    println(sorted.collect.toBuffer)

  }
}

class Dog1(val name: String, val faceValue: Int, val age: Int) extends Ordered[Dog1] with Serializable {
  override def compare(that: Dog1): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    } else {
      this.faceValue - that.faceValue
    }
  }

  override def toString: String = "name:" + name + " faceValue:" + faceValue + " age:" + age
}
