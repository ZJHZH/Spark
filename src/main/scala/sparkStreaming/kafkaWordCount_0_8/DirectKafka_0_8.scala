//package sparkStreaming.kafkaWordCount_0_8
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * date 2019-10-23 11:01<br/>
// * Direct 直连
// *
// * @author ZJHZH
// */
//object DirectKafka_0_8 {
//  def main(args: Array[String]): Unit = {
//    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("ReceiverKafka_0_8").setMaster("local[*]"),Seconds(5))
//
//    // 需要进行全局计算，设置Checkpoint
//    ssc.checkpoint("checkpoint3")
//
//    // 创建一个集合，存topic（为了防止有多个topic）
//    val topicsSet: Set[String] = Set("test1")
//
//    // 需要指定Kafka集群
//    val kafkaParams: Map[String, String] = Map[String,String]("metadata.broker.list" -> "hadoop04:9092,hadoop05:9092,hadoop06:9092")
//
//    // 创建，必须指定泛型，k的数据类型，v的数据类型，k数据类型的序列化，v数据类型的序列化
//    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)
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
