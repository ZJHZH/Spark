package day06

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-15 10:38<br>
 * SQL风格语法：
 * 如果使用SQL风格的语法进行DataFrame操作，必须注册成表
 *
 * @author ZJHZH
 */
object SparkSQL_SQL {
  private val conf: SparkConf = new SparkConf().setAppName("SparkSQL_SQL").setMaster("local[*]")
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {
    // 读文件
    val frame: DataFrame = session.read.json("dir/people.json")
    // 创建临时表
    // 临时表使用session范围内的，只要session退出，表就失效了
    // 只要SparkSession结束，表就自动删除
    frame.createGlobalTempView("people1")
    // 使用GlobalTemp创建表，必须使用global_temp.表名才可以访问
    session.sql("select * from global_temp.people1").show()

    frame.createOrReplaceTempView("people2")
    session.sql("select * from people2").show()

    /**
     * 注册表的时候分为两种：
     *
     * GlobalTemp：全局表，在整个范围内都可以进行使用，只要不退出SparkSession就可以一直访问，
     * 访问时必须添加库名：global_temp.表名，它支持不同的session进行访问。
     * 在同一个文件中，无论创建多少个session对象，都可以访问同一张表。
     * 相对使用较少
     *
     * ReplaceTemp：局部表（临时表），只能是哪个session对象创建的，哪个session对象才可以访问
     * 相对使用较多。
     *
     * 共同点：一旦让创建表的session对象停止，即session.stop()，这个表就会失效
     */

    session.close()
  }
}
