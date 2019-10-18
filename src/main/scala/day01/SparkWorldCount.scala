package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2019-10-08 9:53
 * @author ZJHZH
 */
object SparkWorldCount {
  def main(args: Array[String]): Unit = {
    // 此时需要使用spark对数据进行一些处理，就需要使用一个对象，这个对象可以使用spark提供的一些算子
    /*
    setMaster方法的主要作用
    实际开发中setMaster中可以进行本地程序运行，不打jar包就可以在idea中运行，并且可以进行程序的调试，
    当测试没有问题，会将setMaster删除，将程序打jar包上传集群
    setMaster中参数的传递  三种方式（字符串）
    "local"       本地模式，单线程
    "local[数字]"   本地模式，[数字]线程
    "local[*]"    本地模式，系统空闲的线程，无限接近集群模式，一般用于打jar包上传前的最后一次数据测试
     */
    // SparkContext是Spark中的核心（触发算子计算RDD中的数据）需要SparkConf队形作为参数进行传入
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
    val context: SparkContext = new SparkContext(conf)

    // 读取文件 参数有两个 第一个是读取文件路径 第二个参数是分区大小，参数可以不写，因为有默认参数存在
    val lines: RDD[String] = context.textFile(args(0))

    // 数据处理，切分数据
    val words: RDD[String] = lines.flatMap((_: String).split(" "))

    // 将每一个单词生成一个元组
    val tuples: RDD[(String, Int)] = words.map(((_: String),1))

    // reduceByKey    相同key为一组，进行进行计算
    val sumed: RDD[(String, Int)] = tuples.reduceByKey((_: Int) + (_: Int))

    // 需要对整个RDD中存储的结果进行排序，此时可以调用的第一个排序方法就是sortBy
    // 但是这个sortBy和Scala中的sortBy是不一样的
    // Spark中的sortBy可以指定排序的类型（升/降） Scala中sortBy只能升序
    // 第一个参数必须传递，是根据谁来排序
    // 第二个参数是非必须传递，默认是true代表升序，false代表降序
    // 第三个参数设置分区
    val sorted: RDD[(String, Int)] = sumed.sortBy((_: (String, Int))._2,ascending = false)

    // 以上皆为懒加载，结果就在内存中，但是没有保存，也就找不到这个数据，只有将数据指定保存，才会“有”结果产生
    sorted.saveAsTextFile(args(1))

    // 停止context，结束任务
    context.stop()
  }
}
