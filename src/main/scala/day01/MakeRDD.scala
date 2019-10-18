package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-08 14:12
 *
 * @author ZJHZH
 */
object MakeRDD {
  def main(args: Array[String]): Unit = {
    // 创建RDD需要SparkContext对象
    val context: SparkContext = new SparkContext(new SparkConf().setAppName("MakeRDD").setMaster("local"))

    // 创建RDD，参数有两个，第一个参数必须传，是存储数据的集合；第二个参数不是必须传递的，是具体的分区数
    val rdd1: RDD[Int] = context.makeRDD(Array(1,2,3,4,5))

    // 这两种方式都是用来模拟数据进行计算
    val rdd2: RDD[Int] = context.parallelize(Array(1,3,4,2,5))

    // 实际开发中RDD的产生是由外部数据决定
    // 外部数据中获取RDD
    val rdd3: RDD[String] = context.textFile(args(0))



  }
}
