//package sparkStreaming.kafkaWordCount_0_8
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * date 2019-10-23 11:25<br/>
// * 0.8版本API的手动管理OffSet
// *
// * @author ZJHZH
// */
//object DirectOffSetZookeeper_0_8 {
//  def main(args: Array[String]): Unit = {
//    val ssc: StreamingContext = new StreamingContext(new SparkConf().setAppName("ReceiverKafka_0_8").setMaster("local[*]"),Seconds(5))
//
//    // 需要进行全局计算，设置Checkpoint
//    ssc.checkpoint("checkpoint4")
//
//    // 进行配置0.8使用下面这种方式，0.10版本使用0.10案例中配置替换即可
//    // 组名
//    val groupID: String = "group01"
//
//    // topic
//    val topic: String = "test1"
//
//    // 指定kafka集群地址
//    val brokerList: String = "hadoop04:9092,hadoop05:9092,hadoop06:9092"
//
//    // zk连接地址
//    val zks: String = "hadoop05:2181"
//
//    // 创建topic集合
//    val topics: Set[String] = Set(topic)
//
//    // 编写kafka参数
//    val kafkas: Map[String, String] = Map(
//      "metadata.broker.list" -> brokerList,
//      "group.id" -> groupID,
//      // 消费方式（offset是保存在zk中）
//      // 0.8中这个值有两个（largest/smallest）
//      // largest表示接受最大的offset（即最新的数据）
//      // smallest表示最小的offset（即从topic的开始位置消费所有数据）
//      // 例如：新的groupID或者zk数据被清空，consumer应该从哪个offset开始消费就由这两个参数决定
//      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
//    )
//
//    // 第一步获取zk里面offSet
//    // 第二步通过offset获取kafka的数据
//    // 第三个将处理后的数据offset进行保存，并更新zk
//
//    // 获取offset的工作路径
//    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupID,topic)
//
//    // 再获取zk中consumer的offset路径
//    val zkTopicPath: String = topicDirs.consumerOffsetDir
//
//    // 查看该路径下是否有子目录，并且指定读取数据的序列化方式
//    /*
//    1.zk集群，一个节点即可
//    2.session连接时长
//    3.connection连接时长（超时时长）
//    4.读取数据的序列化
//     */
//    val zkClient: ZkClient = new ZkClient(zks,Integer.MAX_VALUE,10000,ZKStringSerializer)
//    val client: Int = zkClient.countChildren(zkTopicPath)
//
//    // 创建输入流
//    var kafkaStream:InputDStream[(String,String)] = null
//
//    // 存储topic 和partition的个数
//    var fromOffset:Map[TopicAndPartition,Long] = Map()
//
//    // 是否访问过zk内部（是否访问过topic）
//    if (client > 0){
//      // 获取每个partition下面的offset
//      for (i <- 0 until client){
//        // 取出offset
//        val partitionOffset: Nothing = zkClient.readData(s"${zkTopicPath}/${i}")
//
//        // 加载不同分区的offset
//        val topicAndPartition: TopicAndPartition = TopicAndPartition(topic,i)
//
//        // 将数据加载进去
//        fromOffset += (topicAndPartition -> partitionOffset.toString.toLong)
//      }
//      // 创建需要的函数写入数据
//      val messageHandler: MessageAndMetadata[String, String] => (String, String) = (mmd:MessageAndMetadata[String,String])=>{
//        (mmd.key(),mmd.message())
//      }
//
//      // 获取kafka中的数据
//      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String, String)](
//        ssc,
//        kafkas,
//        fromOffset,
//        messageHandler
//      )
//    }else{
//      // 如果没有读取过数据，没有保存的偏移量，根据kafka的配置，从开始的位置读取数据，也就是offset=0
//      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkas,topics)
//    }
//    // 获取offset范围
//    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
//
//    // 首先获取offset每一条数据，就更新内部的offset，然后通过这个方法将最终的offset保存到zk
//    kafkaStream.foreachRDD((rdd: RDD[(String, String)]) =>{
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      // 更新offset
//      for (o <- offsetRanges){
//        // 取值（路径）
//        val zkPaths: String = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//
//        // 将对应partitionOffset进行更新
//        ZkUtils.updatePersistentPath(zkClient,zkPaths,o.untilOffset.toString)
//      }
//    })
//
//    // 启动
//    ssc.start()
//
//    // 等待任务
//    ssc.awaitTermination()
//  }
//}
