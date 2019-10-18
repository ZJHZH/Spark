package day07

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * date 2019-10-17 10:42<br>
 *
 * @author ZJHZH
 */
object HomeWork {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("HomeWork").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val frame: DataFrame = sc.textFile("dir/dept").map(e => {
      val strings: Array[String] = e.split(",")
      dept(strings(0).toInt, strings(1), strings(2))
    }).toDF()

//    frame.show()

    frame.createOrReplaceTempView("dept")

    spark.sql("select * from dept").show()

    
  }
}

case class dept(deptno:Int,dname:String,ioc:String)
