package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * date 2019-10-19 11:44<br>
 *
 * @author ZJHZH
 */
object KafkaProducerDemo {
  def main(args: Array[String]): Unit = {
    // 1.先创建配置列表
    val properties: Properties = new Properties()

    // 指定Kafka集群列表
    properties.put("bootstrap.server","hadoop04:9092,hadoop05:9092,hadoop06:9092")

    // 指定响应方式
    properties.put("acks","all")

    // 请求失败之后重试的次数
    properties.put("retries","3")

    // 指定Key的序列化方式，key用于存储方法数据的对应的OffSet
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")

    // 指定value的序列化方式
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    // 得到生产者对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)

    // 模拟一些数据发送给kafka
    for (i <- 1 to 10000){
      val msg: String = "{" + i + "}: this is kafka data"

      // 发送数据
      producer.send(new ProducerRecord[String,String]("test",msg))

      Thread.sleep(500)
    }
  }
}
