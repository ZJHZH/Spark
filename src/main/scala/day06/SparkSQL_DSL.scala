package day06

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-15 10:19<br>
 *
 * @author ZJHZH
 */
object SparkSQL_DSL {
  private val session: SparkSession = SparkSession.builder().appName("SparkSQL_DSL").master("local[*]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val frame: DataFrame = session.read.json("dir/people.json")
    frame.show()
    // DSL风格
    // 1.算子
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)
    // 打印schema信息
    frame.printSchema()

    // 2.查询，会得到返回值，是一个全新的DataFrame对象
    frame.select("name").show()

    // 3.查询，如果需要使用$ 必须进行隐式转换包的导入
    import session.implicits._
    // $相当于去除字段中所有的值
    frame.select($"name", $"age" + 1).show()
    // filter相当于是select + where语句的结合体
    frame.filter($"age" > 21).show()
    // 相当于是select + count + groupBy
    frame.groupBy("age").count().show()
  }
}
