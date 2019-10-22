package sparkStreaming.worldCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * date 2019-10-22 10:19<br/>
 * 写SparkString程序的时候，不能使用local（单线程本地模式）只能完成一件事（要么接受，要么执行）<br/>
 * 建议使用local[2]或者local[*]模拟多个线程执行，
 * 原因在于SparkStreaming需要一个线程接收数据，另外一个线程长期执行任务
 *
 * @author ZJHZH
 */
object WorldCount {
  def main(args: Array[String]): Unit = {
    // 如果想创建SparkSteaming对象需要使用一个类StreamingContext
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("").setMaster("local[2]"),Seconds(5))
  }
}
