//package sparkStreaming.kafkaWordCount_0_8
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * date 2019-10-23 10:41<br/>
// * 0.8版本KafkaAPI
// *
// * @author ZJHZH
// */
//object ReceiverKafka_0_8 {
//  def main(args: Array[String]): Unit = {
//    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("ReceiverKafka_0_8").setMaster("local[*]"),Seconds(5))
//
//    // 需要进行全局计算，设置Checkpoint
//    ssc.checkpoint("checkpoint2")
//
//    // 在进行kafka连接的时候，KafkaAPI中提供了一个工具类
//    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
//      ssc,
//      "hadoop04:2181",
//      "group01",
//      Map("test1" -> 5)
//    )
//
//    // 数据处理
//    val tuples: DStream[(String, Int)] = kafkaDStream.flatMap((e: (String, String)) => e._2.split(" ")).map(((_: String), 1))
//
//    // 对数据进行累加
//    val sum: DStream[(String, Int)] = tuples.updateStateByKey((values: Seq[Int], state: Option[Int])=>Option(values.size + state.getOrElse(0)))
//
//    sum.print()
//
//    ssc.start()
//
//    ssc.awaitTermination()
//  }
//}
