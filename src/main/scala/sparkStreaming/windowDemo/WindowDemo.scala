package sparkStreaming.windowDemo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * date 2019-10-23 9:40<br/>
 * SparkStreaming窗口滑动
 *
 * @author ZJHZH
 */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("WindowDemo").setMaster("local[2]"),Seconds(5))

    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop04",6666)

    val tuples: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_,1))

    // 提供窗口
    // tuples.window(Seconds(10),Seconds(10))

    val sum: DStream[(String, Int)] = tuples.reduceByKeyAndWindow((_:Int)+(_:Int),Seconds(10),Seconds(10))

    sum.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
