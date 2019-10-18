package day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * date 2019-10-14 16:22<br>
 * 通过反射获取Schema信息
 * @author ZJHZH
 */
object RDD2DataSet {
  private val conf: SparkConf = new SparkConf().setAppName("RDD2DataSet").setMaster("local[*]")
  private val sc: SparkContext = new SparkContext(conf)
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {
    val value: RDD[String] = sc.textFile("dir/people.txt")

    // 开始转换
    // 使用toDS和使用toDF一样需要使用隐式转换
    import session.implicits._

    val s: Dataset[People2] = value.map(e => {
      val strings: Array[String] = e.split(",")
      People2(strings(0), strings(1).trim.toInt)
    }).toDS

    s.show()

    // DataSet转DataFrame，转换方便，相当于触发了case class中的属性（隐式转换，上面导了包的）
    val frame: DataFrame = s.toDF()

    // DataFrame转DataSet，使用as[U]转换，并提供样例类，样例类中的字段名随意，但是数据类型必须和DataFrame一致
    val value1: Dataset[People2] = frame.as[People2]

    // DataSet转RDD，更方便
    val rdd: RDD[People2] = s.rdd

    sc.stop()
    session.close()
  }

}

case class People2(name:String,age:Int)
