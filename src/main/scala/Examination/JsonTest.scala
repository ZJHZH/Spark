package Examination

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * date 2019-10-19 15:18<br>
 *
 * @author ZJHZH
 */
object JsonTest {
  private val spark: SparkSession = SparkSession.builder().appName("JsonTest").master("local").enableHiveSupport().getOrCreate()
  def main(args: Array[String]): Unit = {
    val frame: DataFrame = spark.read.json("dir/Examination/JsonTest02.json")
    frame.createOrReplaceTempView("jt")

    spark.sql(
      """
        |select *
        |from (
        |select
        |phoneNum,
        |sum(if(status=1,money,0)) as sum_money
        |from jt
        |group by phoneNum
        |) tmp
        |order by sum_money desc
        |""".stripMargin).repartition(1).write.json("out/sum_money_desc")

    spark.sql(
      """
        |select *
        |from (
        |select
        |terminal,
        |count(1) as login_num
        |from jt
        |group by terminal
        |) tmp
        |order by login_num desc
        |""".stripMargin).repartition(1).write.json("out/login_num_desc")

    import spark.implicits._
    spark.sql(
      """
        |select
        |province,
        |phoneNum,
        |count(1) as login_num
        |from jt
        |group by province,phoneNum
        |""".stripMargin)
      .rdd.map(e=>(e.getString(0),(e.getString(1),e.getLong(2))))
      .groupByKey(1) // 200个分区，受不了
      .map(e=>top3(e._1,e._2.toList.sortBy(_._2).takeRight(3).map(_._1).reduce(_+","+_)))
      .toDF.write.json("out/top3")

    spark.stop()
  }
}

case class top3(province:String,phoneNums:String)
