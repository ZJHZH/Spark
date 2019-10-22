package sparkStreaming.TransFormDemo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * date 2019-10-22 15:35<br/>
 * 黑名单，过滤敏感字段
 *
 * @author ZJHZH
 */
object Demo {
  def main(args: Array[String]): Unit = {
    // 初始化SparkConf，并创建StreamingContext对象
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("TransFormDemo").setMaster("local[2]"),Milliseconds(5000))

    // 设置Checkpoint
    ssc.checkpoint("checkpoint1")

    // 获取数据源
    val wcDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop04",6666)

    val wc: DStream[(String, Int)] = wcDS.flatMap(_.split(" ")).map((_,1))

    // 模拟一个数据源
    val filters: RDD[(String, Boolean)] = ssc.sparkContext.parallelize(List("ni","wo","ta")).map((_,true))

    // 使用模拟黑名单数据进行对存储在DStream中的RDD进行对比
    val words: DStream[(String, Int)] = wc.transform((rdd: RDD[(String, Int)]) => {
      // 做合并
      //      val value: RDD[(String, (Option[Boolean], Int))] = filters.rightOuterJoin(rdd)
      val value: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(filters)

      // 过滤黑名单
      //      val filter: RDD[(String, (Option[Boolean], Int))] = value.filter(!_._2._1.getOrElse(false))
      val filter: RDD[(String, (Int, Option[Boolean]))] = value.filter(!_._2._2.getOrElse(false))

      filter.map(e => (e._1, e._2._1))
    })

    val worldCount: DStream[(String, Int)] = words.updateStateByKey((x: Seq[Int],y: Option[Int]) =>Option(x.sum+y.getOrElse(0)))

    worldCount.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
