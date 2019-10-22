package sparkStreaming.RDDStream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * date 2019-10-22 11:23<br/>
 * RDD队列输入
 *
 * @author ZJHZH
 */
object RDDStream {
  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息并创建SparkStreaming对象
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("RDDStream").setMaster("local[2]"),Seconds(5))

    // 2.创建RDD队列
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    // 3.读取队列中数据
    // 第二个参数，是否在该间隔内队列中仅使用一个RDD
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,oneAtATime = false)

    // 4.因为RDD中存储的Int，所以直接拼接数据即可
    val sumed: DStream[(Int, Int)] = inputStream.map((_,1)).reduceByKey(_+_)

    sumed.print()

    // 启动任务
    ssc.start()

    // 循环模拟数据
    for (i <- 1 to 10){
      ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
