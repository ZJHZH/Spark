package sparkStreaming.kafkaWordCount_0_8

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
 * date 2019-10-23 10:34<br/>
 *
 * @author ZJHZH
 */
object Producter {
  def main(args: Array[String]): Unit = {
    // 1.先创建配置列表
    val properties: Properties = new Properties()

    // 指定Kafka集群列表
    properties.put("bootstrap.servers","hadoop04:9092,hadoop05:9092,hadoop06:9092")

    // 指定响应方式
    properties.put("acks","all")

    // 请求失败之后重试的次数
    properties.put("retries","3")

    // 指定Key的序列化方式，key用于存储方法数据的对应的OffSet
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")

    // 指定value的序列化方式
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    // 创建producer对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)

    // 是数据存储（模拟数据存储）
    val strings: Array[String] = Array("hello tom","hello jerry","hello kitty","hello suke")

    val random: Random = new Random()

    while (true){
      // 随机在数据中获取数据当做消息发送给kafka
      val str: String = strings(random.nextInt(strings.length))

      producer.send(new ProducerRecord[String,String]("test1",str))

      Thread.sleep(500)
    }
  }
}
