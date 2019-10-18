package day07

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-17 10:16<br>
 *
 * @author ZJHZH
 */
object SparkSQLOutputJDBC {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SparkSQLOutputJDBC").master("local").getOrCreate()
    // 读取文件数据
    val frame: DataFrame = session.read.json("dir/employees.json").cache()

    // 方式一 Properties形式
    val properties: Properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")
    // 表可以不存在，通过读取数据可以直接生成表
    frame.write.jdbc("jdbc:mysql://localhost:3306/db1901?useSSL=false","employees1",properties)

    // 方式二
    frame.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/db1901?useSSL=false")
      .option("dbtable","employees2")
      .option("user","root")
      .option("password","root")
      .save()

    // 方式三，执行创建表的列名和数据类型，数据类型不能大写
    frame.write.option("createTableColumnTypes","name varchar(200),salary int")
      .jdbc("jdbc:mysql://localhost:3306/db1901?useSSL=false","employees3",properties)

    session.stop()
  }
}
