package day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-14 15:24<br>
 *
 * @author ZJHZH
 */
object RDD2DataFrame {
  private val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local[*]")
  private val sc: SparkContext = new SparkContext(conf)
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {

    val lineRDD: RDD[Array[String]] = sc.textFile("dir/people.txt").map(_.split("[^\\w]+"))
    val tuple: RDD[(String, String)] = lineRDD.map(x=>(x(0),x(1)))

    // 转换过程：
    // 先构建SparkSession对象
    // 需要将RDD转换为DataFrame，呢么需要引用一个隐式转换包
    import session.implicits._
    // toFf是RDD转换为DataFrame的一种方式，可以直接输入字段名
    val frame: DataFrame = tuple.toDF("name","age")
    frame.show()

    sc.stop()
    session.close()
  }
}






















