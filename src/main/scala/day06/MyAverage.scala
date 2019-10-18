package day06

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

/**
 * date 2019-10-15 11:48<br>
 * 自定义UDAF函数，DataFrame版本
 * @author ZJHZH
 */
class MyAverage extends UserDefinedAggregateFunction{
  // 输入的数据
  override def inputSchema: StructType = StructType(List(StructField("Salary",DoubleType)))

  // 每一个分区中的共享变量，用来存储记录的值
  override def bufferSchema: StructType = {
    StructType(StructField("sum",DoubleType)::StructField("count",DoubleType)::Nil)
  }

  // 返回数据类型的表示，即表示UDAF输出值的类型
  override def dataType: DataType = DoubleType

  // 如果有相同的输入，UDAF函数是否有相同的输出，是则true，否则false
  override def deterministic: Boolean = true

  // 对分区数据进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存储工资的总额
    buffer(0) = 0.0

    // 存储工资的总个数
    buffer(1) = 0.0
  }

  // 分区内聚合，相同Executor之间数据的合并，合并每一个分区中数据时进行计算
  // buffer相当共享变量
  // input是一行数据，即读取到数据以行看待
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)){
      // 进行工资的累加，统计累加工资的个数
      // 第一个getDouble相当于获取sum的值
      // 第二个getDouble相当于获取读取数据中的工资
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)

      // 求一共有多少个工资累加
      buffer(1) = buffer.getDouble(1) + 1
    }
  }

  // 全局聚合，将所有数据进行累加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 合并所有工资
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)

    // 合并所有工资的个数
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Double = {
    buffer.getDouble(0) / buffer.getDouble(1)
  }
}

object MyAverage{
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("MyAverage").master("local[*]").getOrCreate()
    val frame: DataFrame = session.read.json("dir/employees.json")
    frame.createOrReplaceTempView("employees")
    session.udf.register("myaverage",new MyAverage)

    session.sql("select * from employees").show()

    session.sql("select myaverage(salary) as avg_salary from employees").show()


    session.stop()
  }
}
