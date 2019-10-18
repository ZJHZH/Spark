package day07

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-17 9:44<br>
 * 从JDBC中获取数据
 *
 * @author ZJHZH
 */
object SparkSQLInputJDBC {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName("SparkSQLInputJDBC").master("local").getOrCreate()
    // 第一种读取方式
    // 需要创建Properties文件
    val properties: Properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")

    val frame: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/db1901","emp",properties)
    frame.show()
    //第二种写法
    spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/db1901")
      .option("user","root").option("password","root")
      .option("dbtable","emp").load().show()
  }
}
