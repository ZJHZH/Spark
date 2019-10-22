package sparkStreaming.worldCount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * date 2019-10-22 10:19<br/>
 * 无转换状态<br/>
 * 写SparkString程序的时候，不能使用local（单线程本地模式）只能完成一件事（要么接受，要么执行）<br/>
 * 建议使用local[2]或者local[*]模拟多个线程执行，
 * 原因在于SparkStreaming需要一个线程接收数据，另外一个线程长期执行任务
 *
 * @author ZJHZH
 */
object WorldCount {
  def main(args: Array[String]): Unit = {
    // 如果想创建SparkSteaming对象需要使用一个类StreamingContext
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("WorldCount").setMaster("local[2]"),Seconds(5))

    // 获取实时数据，从netcat服务器端获取数据
    // 需要传入对应IP地址和对应端口号即可以获取到数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop04",6666)
    // 处理DStream中存储的批次数据，操作和RDD基本上没什么区别，可以直接和RDD操作方式来操作DStream
    // 需要注意的是：当前算子在DStream含义不同
    val dStream1: DStream[(String, Int)] = dStream.flatMap((_: String).split(" ")).map(((_: String),1)).reduceByKey((_: Int)+(_: Int))

    // 将结果直接打印到控制台
    dStream1.print()

    // 开启任务
    ssc.start()

    // 等待任务，获取下一次批次数据
    ssc.awaitTermination()
  }
}
