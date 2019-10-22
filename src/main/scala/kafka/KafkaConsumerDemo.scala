package kafka

import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}


/**
 * date 2019-10-19 12:00<br>
 *
 * @author ZJHZH
 */
object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    // 1.先创建配置列表
    val properties: Properties = new Properties()

    // 指定Kafka集群列表
    properties.put("bootstrap.servers","hadoop04:9092,hadoop05:9092,hadoop06:9092")

    // 指定响应方式
    properties.put("acks","all")

    properties.put("group.id","group01")

    properties.put("auto.offset.reset","earliest")

    // 指定Key的反序列化方式，key用于存储方法数据的对应的OffSet
    properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    // 指定value的反序列化方式
    properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    // 得到消费者对象
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String,String](properties)

    // 需要订阅topic
    consumer.subscribe(Collections.singletonList("test1"))

    // 开始消费数据
    while (true){
      val msgs: ConsumerRecords[String, String] = consumer.poll(1000)
      val iter: util.Iterator[ConsumerRecord[String, String]] = msgs.iterator()
      while (iter.hasNext){
        val msg: ConsumerRecord[String, String] = iter.next()
        println("partition:"+msg.partition()+",offset:"+msg.offset()+",key:"+msg.key()+",value:"+msg.value())
      }
    }
  }
}
