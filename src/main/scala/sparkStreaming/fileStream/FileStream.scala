package sparkStreaming.fileStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * date 2019-10-22 11:00<br/>
 * Streaming监控HDFS集群文件夹，做到实时数据处理
 *
 * @author ZJHZH
 */
object FileStream {
  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息并创建SparkStreaming对象
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("FileStream").setMaster("local[2]"),Seconds(5))

    // 2.设置监控文件夹，创建DStream，高可用必须是Active节点
    val dirStream: DStream[String] = ssc.textFileStream("hdfs://hadoop04:9000/fileStream")

    // 3.整体处理逻辑不变
    val dSream1: DStream[(String, Int)] = dirStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    // 4.打印到控制台
    dSream1.print()

    // 开启任务
    ssc.start()

    // 等待任务，获取下一批次的数据
    ssc.awaitTermination()

  }
}
