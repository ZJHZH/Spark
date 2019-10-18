package day06

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-15 15:56<br>
 * 添加依赖jar包
 *
 * @author ZJHZH
 */
object SparkSQL_HiveOnSparkLocal {
  def main(args: Array[String]): Unit = {
    // 因为是本地hive，所以需要指定存储位置
    val spark: SparkSession = SparkSession.builder().appName("SparkSQL_HiveOnSparkLocal").config("spark.sql.warehouse.dir","E://spark_warehouse").master("local").enableHiveSupport().getOrCreate()

    // 创建一张表
    spark.sql("create table if not exists src_1(key Int,value String)")

    // 向表中插入数据
    spark.sql("load data local inpath 'dir/kv1.txt' into table src_1")

    // 查询表中结果
    val frame: DataFrame = spark.sql("select * from src_1")
    frame.show()

    // 删表
    spark.sql("drop table if exists src_1")

    spark.stop()
  }
}
