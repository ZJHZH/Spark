package StructuredStreamingDemo

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * date 2019-10-23 14:57<br/>
 * 新的实时流
 *
 * @author ZJHZH
 */
object Demo {
  def main(args: Array[String]): Unit = {
    // 当前Streaming是一个接近完全实时的流，省略了批次间隔，此时只需要一个线程也行
    val spark: SparkSession = SparkSession.builder().appName("Demo").master("local").getOrCreate()

    val lines: DataFrame = spark.readStream.format("socket").option("host","hadoop04").option("port",6666).load()

    // 将当前DataFrame转化为DataSet
    import spark.implicits._
    val linesDS: Dataset[String] = lines.as[String]

    // 拆分
    val words: Dataset[String] = linesDS.flatMap(_.split(" "))

    // 求和
    val sumed: DataFrame = words.groupBy("value").count()

    val query: StreamingQuery = sumed.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
