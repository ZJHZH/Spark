package sparkStreaming.kafkaWordCount_0_10

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * date 2019-10-23 14:11<br/>
 * 使用的是kafkaAPI0.10
 *
 * @author ZJHZH
 */
object DirectKafkaDemo {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("DirectKafkaDemo").setMaster("local[*]"),Seconds(5))

    ssc.checkpoint("checkpoint4")

    // 指定kafka的配置需求，必须是Map集合
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop04:9092,hadoop05:9092,hadoop06:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group01",
      /* 指定当前消费位置
       当各个分区下已有提交的offset时，从提交的offset开始消费--earliest（最新位置）
       无提交offset时 ，从头开始消费--latest
       无提交offset时，消费新产生该分区下的数据，使用none（只要有一个分区提交直接抛异常）
       */
      "auto.offset.reset" -> "latest",
      // 如果value合法，自动提交offset
      "enable.auto.commit" -> (true:lang.Boolean)
    )
    // 创建存储topic集合
    val topics: Array[String] = Array("test1")

    val logs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      /*
      消费数据的本地策略
      PreferBrokers
      仅仅在spark的Executor在相同节点，优先分配到存才kafkaBroker的机器上
      PreferConsistent
      大多数情况下，一致性的方式分配给所有分区上Executor（主要是均匀分配）
       */
      LocationStrategies.PreferBrokers,
      // kafka是订阅模式，相当于是推送数据（自身拉取）
      // 消费哪个topic，kafka集群配置
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )

    val lines: DStream[String] = logs.map(_.value())

    val tuples: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    tuples.updateStateByKey((x: Seq[Int], y: Option[Int])=>{
      Option(x.sum + y.getOrElse(0))
    })
  }
}
