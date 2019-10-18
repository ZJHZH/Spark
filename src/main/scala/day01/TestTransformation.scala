package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-08 14:33<br>
 * 简单算子演示
 *
 * @author ZJHZH
 */
object TestTransformation {
  def main(args: Array[String]): Unit = {
    // 创建SparkContext对象
    val conf: SparkConf = new SparkConf().setAppName("TestTransformation").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[Int] = context.parallelize(List(1,2,4,2,4,6,5,4,1))

    // 遍历RDD中每一个元素并进行操作，参数是一个RDD中的函数
    val rdd2: RDD[Int] = rdd.map(_ * 2)
    println(rdd2.collect().toBuffer)
  }
}
