package WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-09 13:13<br>
 *
 * @author ZJHZH
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val words: RDD[String] = lines.flatMap(_.split("[^\\w]+"))
    val tuples: RDD[(String, Int)] = words.map((_,1))
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2,false)
    sorted.saveAsTextFile(args(1))
    sc.stop()
  }
}
