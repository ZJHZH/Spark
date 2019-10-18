package day06

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-15 16:12<br>
 * 必须开启集群HDFS 和 Spark集群
 * 将hive-site.xml添加到resources文件夹中
 * 添加JDBC连接驱动，在pom文件中配置即可
 * @author ZJHZH
 */
object SparkSQL_HiveOnSparkMaster {
  def main(args: Array[String]): Unit = {
    // 因为是本地hive，所以需要指定存储位置
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkSQL_HiveOnSparkMaster")
      // 这句可以不写
      .config("spark.sql.warehouse.dir","hdfs://hadoop04:9000/spark_warehouse")
      .master("spark://hadoop04:7077")  // master节点
      .enableHiveSupport()
      .getOrCreate()

    // 创建一张表
    spark.sql("create table if not exists src_1901(key Int,value String)")

    // 向表中插入数据
    spark.sql("load data local inpath 'dir/kv1.txt' overwrite into table src_1901")

    // 查询表中结果
    spark.sql("select * from src_1901").show()

    // 删表
    spark.sql("drop table if exists src_1901")

    spark.stop()
  }
}
