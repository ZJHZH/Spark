package sparkStreaming.customer

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * date 2019-10-22 11:48<br/>
 * 自定义获取数据
 *
 * @author ZJHZH
 */
object CustomerStream {
  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息并创建SparkStreaming对象
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("FileStream").setMaster("local[2]"),Seconds(5))

    // 需要使用自定receiver 的Streaming
    val stream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop04",6666))

    // 3.整体处理逻辑不变
    val dSream1: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    // 4.打印到控制台
    dSream1.print()

    // 开启任务
    ssc.start()

    // 等待任务，获取下一批次的数据
    ssc.awaitTermination()
  }
}
