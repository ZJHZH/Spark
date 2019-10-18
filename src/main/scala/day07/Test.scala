package day07

import org.apache.spark.sql.SparkSession

/**
 * date 2019-10-17 9:02<br>
 *
 * @author ZJHZH
 */
object Test {
  private val spark: SparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    // 这三种读法是SparkSession最常用的，特别是json格式
    //    spark.read.json()
    //    spark.read.jdbc()
    //    spark.read.csv()

    // 读文本文件
    //    spark.read.text()
    //    spark.read.textFile()

    // 压缩格式，支持SparkSQL的全部数据类型
    //    spark.read.parquet()
    //    spark.read.orc()

    // table，直接读表，很少用
    //    spark.read.table()

    // 第二种写法，沿用之前的Spark1.6版本
    //    spark.read.format("文件类型").load("位置")
    // spark.read.format("json").load("位置")

    // 若不知型默认是parquet
    //    spark.read.load()

  }
}
