package day06

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
 * date 2019-10-15 14:35<br>
 *
 * @author ZJHZH
 */
class MyAverage2 extends Aggregator[Employee,Average,Double]{
  // 初始化分区共享变量
  override def zero: Average = Average(0.0,0.0)

  // 分区内聚合
  override def reduce(b: Average, a: Employee): Average = {
    b.sum += a.salary
    b.count += 1
    b
  }

  // 分区之间的聚合
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  // 计算最终结果
  override def finish(reduction: Average): Double = reduction.sum / reduction.count

  // 设置类型解码器，要转换成case类，Encoders.product是进行scala元组和case类类型转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 设置最终输出的解码器，获取的是Scala中的数据类型
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class Employee(name:String,salary:Double)
case class Average(var sum: Double, var count: Double)

object MyAverage2{
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("MyAverage2").master("local[*]").getOrCreate()

    // 需要导入隐式转换包
    import session.implicits._
    val ds: Dataset[Employee] = session.read.json("dir/employees.json").as[Employee]

    ds.show()
    // toColumn 作用是获取对应的类名，相当于获取的是case class Employee(name:String,salary:Double)
    // 此时的name中存储着读取数据中对应列名
    // name的参数是一个别名（可以不写）
    val average: TypedColumn[Employee, Double] = new MyAverage2().toColumn.name("avg_salary")
    val res: Dataset[Double] = ds.select(average)
    res.show()
  }
}
