package day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * date 2019-10-14 15:50<br>
 * 通过StructType直接指定Schema
 * @author ZJHZH
 */
object RDD2DataFrame_3 {
  private val conf: SparkConf = new SparkConf().setAppName("RDD2DataFrame_3").setMaster("local[*]")
  private val sc: SparkContext = new SparkContext(conf)
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {
    val lineRDD: RDD[Array[String]] = sc.textFile("dir/people.txt").map(_.split(","))

    val tuple: RDD[(String, Int)] = lineRDD.map(x=>(x(0),x(1).trim.toInt))

    // 进行转换

    // 创建一个Schema信息
    val structType: StructType = StructType(
      List(
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      )
    )
    structType

    // 不能直接生成元组，需要生成Row类型
    val rowRDD: RDD[Row] = lineRDD.map(p=>Row(p(0),p(1).trim.toInt))

    // 将Schema信息应用到RowRDD上
    val frame: DataFrame = session.createDataFrame(rowRDD,structType)

    val frame1: DataFrame = frame.toDF()

    frame1.show()

    sc.stop()
    session.close()
  }
}













