package day05

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * SparkSession的三种创建方式
 * date 2019-10-14 14:56<br>
 *
 * @author ZJHZH
 */
object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    // 方式一，使用builder构建，appName设置App名字，master设置运行模式，getOrCreate真正创建
    val session1: SparkSession = SparkSession.builder().appName("SparkSQLDemo").master("local[*]").getOrCreate()
    // 方式二，需要先创建SparkConf对象，将配置信息写入到Conf对象中，然后在创建SparkSession时使用config传入conf
    val session2: SparkSession = SparkSession.builder().config(new SparkConf().setAppName("SparkSQLDemo").setMaster("local[*]")).getOrCreate()
    // 方式三，主要是对Hive操作的创建，enableHiveSupport()开启hive支持
    val session3: SparkSession = SparkSession.builder().appName("SparkSQLDemo").master("local[*]").enableHiveSupport().getOrCreate()

    // 用完，必须关闭
    session1.close()
    session2.close()
    session3.close()
  }
}
