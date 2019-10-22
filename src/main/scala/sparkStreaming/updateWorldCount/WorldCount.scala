package sparkStreaming.updateWorldCount

import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer.HashPartition
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * date 2019-10-22 14:21<br/>
 * 可以对批次累加的WorldCount
 *
 * @author ZJHZH
 */
object WorldCount {
  def main(args: Array[String]): Unit = {
    // 初始化SparkConf，并创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("WorldCount").setMaster("local[2]"),Milliseconds(5000))

    // 需要使用状态更新，需要将原有的结果记录下来，才可以再次更新结果
    // 此时SparkStreaming使用的方式是Checkpoint，必须设置检查点，用来存储批次计算完成的结果
    ssc.checkpoint("checkpoint1")

    // 获取监听数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop04",6666)

    // 将数据生成元组方便计算
    val tuples: DStream[(String, Int)] = dStream.flatMap(_.split(" ").map((_,1)))

    // reduceByKey(_+_)只能对当前批次的数据进行统计，不会影响到其他批次
    // 使用updateStateByKey来做数据的状态更新，统计从开始到结束的所有的单词的总数
    val tuples1: DStream[(String, Int)] = tuples.updateStateByKey((x: Seq[Int], y: Option[Int]) => {
      Option(x.sum + y.getOrElse(0))
    })

    tuples.updateStateByKey((e:Iterator[(String,Seq[Int],Option[Int])])=>{
      e.map((x)=> {
        (x._1,x._2.sum+x._3.getOrElse(0))
      })
    },new HashPartitioner(ssc.sparkContext.defaultMinPartitions),rememberPartitioner = true)

    // 控制台打印
    tuples1.print()

    // 启动任务
    ssc.start()

    // 等待任务
    ssc.awaitTermination()
  }
}
