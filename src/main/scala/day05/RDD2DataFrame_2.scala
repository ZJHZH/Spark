package day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

/**
 * date 2019-10-14 15:43<br>
 *
 * @author ZJHZH
 */
object RDD2DataFrame_2 {
  private val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrame_2").setMaster("local[*]")
  private val sc: SparkContext = new SparkContext(conf)
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {
    val lineRDD: RDD[Array[String]] = sc.textFile("dir/people.txt").map(_.split(","))

//    val tuple: RDD[(String, Int)] = lineRDD.map(x=>(x(0),x(1).trim.toInt))
    val tuple: RDD[People1] = lineRDD.map(e=>People1(e(0),e(1).trim.toInt))

    import session.implicits._

    // DF中不传入参数，而是使用反射的形式获取Schema信息，方式是提供一个样例类
    // 此时不需要传入信息，会根据传入的样例类中定义的属性来自动获取字段
    val f: DataFrame = tuple.toDF

    f.show()

    val rdd: RDD[Row] = f.rdd
    val value: RDD[(String, Int)] = rdd.map(e=>(e.getString(0),e.getInt(1)))
    value.foreach(println)

    // DataFrame转DataSet
    val value1: Dataset[People1] = f.as[People1]
    value1.show()

    sc.stop()
    session.close()
  }
}

case class People1(name:String,age:Int)
