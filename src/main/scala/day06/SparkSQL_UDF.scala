package day06

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-15 11:23<br>
 * 自定义UDF函数
 *
 * @author ZJHZH
 */
object SparkSQL_UDF {
  private val session: SparkSession = SparkSession.builder().appName("SparkSQL_UDF").master("local[*]").getOrCreate()
  def main(args: Array[String]): Unit = {
    // 读数据
    val frame: DataFrame = session.read.json("dir/people.json")

    // UDF函数的创建与使用
    // 1.先注册这个函数，注册成功后在整个应用中都可以使用
    // 常用创建方式是：两个参数版本
    // 第一个参数：UDF函数的名称
    // 第二个参数是UDF函数的实现
    // 得到的返回值就是我们需要的UDF函数
    val addName: UserDefinedFunction = session.udf.register("addName",(x:String)=>"name:"+x)

    // 使用，多用于在SQL语句中
    frame.createOrReplaceTempView("people")
    session.sql("select addName(name),age from people").show()

    session.stop()
  }
}
