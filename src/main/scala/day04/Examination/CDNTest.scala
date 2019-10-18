package day04.Examination

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.matching.Regex

/**
 * date 2019-10-12 15:32<br>
 *
 * @author 赵静华
 */
object CDNTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("CDNTest")
    val sc = new SparkContext(conf)

    val input: RDD[(String, String, String, Long, String)] = sc.textFile("dir/cdn.txt")
      .map(e => {
        val strings: Array[String] = e.split(" ")
        val ip: String = strings(0)
        val hour: String = strings(3).replaceFirst(".*?/\\d+:(\\d+):.*", "$1")
        val state: String = strings(8)
        val size: Long = strings(9).toLong
        var file: String = strings(6).replaceFirst(".*?/([\\w]+\\.mp4).*", "$1")
        if (file.equals(strings(6))) file = ""
//        var file: String = strings(10).replaceFirst(".*?[?&]id=(\\w+?)&.*","$1.mp4")
//        if (file.equals(strings(10))) file = ""
        (ip, file, hour, size, state)
      }).cache()

    // 统计独立IP访问量前10位
    ipStatics(input)

    //统计每个视频独立IP数
    videoIpStatics(input)

    // 统计一天中每个小时间的流量
    flowOfHour(input)

    sc.stop()
  }

  //统计一天中每个小时间的流量
  def flowOfHour(data: RDD[(String, String, String, Long, String)]): Unit = {
    println()
    println("统计一天中每个小时间的流量：")
    data.filter(_._5 != "404").map(e=>(e._3,e._4)).reduceByKey(_+_).sortBy(_._1.toInt,ascending = true,1).foreach(e=>{
      val l: Long = e._2 >> 30
      println(e._1 + "时 CDN流量=" + l + "G")
    })
  }

  // 统计每个视频独立IP数
  def videoIpStatics(data: RDD[(String, String, String, Long, String)]): Unit = {
    println("统计每个视频独立IP数：")
//    data.map(e=>((e._2,e._1),1)).filter(_._1._1 != "").distinct(1).map(e=>(e._1._1,e._2)).reduceByKey(_+_).sortBy(_._2,ascending = false,1).foreach(e=>println("视频:"+ e._1 + " 独立IP数:" + e._2))
//    data.map(e=>((e._2,e._1),1)).filter(_._1._1 != "").reduceByKey((x,y)=>x).map(e=>(e._1._1,1)).reduceByKey(_+_).sortBy(_._2,ascending = false,1).foreach(e=>println("视频:"+ e._1 + " 独立IP数:" + e._2))
//    data.map(e=>(e._2,e._1)).filter(_._1 != "").groupByKey().map(e=>(e._1,e._2.toSet.size)).sortBy(_._2,ascending = false,1).foreach(e=>println("视频:"+ e._1 + " 独立IP数:" + e._2))
    data.map(e=>((e._2,e._1),1)).filter(_._1._1 != "").reduceByKey((x,y)=>x).map(e=>(e._1._1,1)).aggregateByKey(0)(_+_,_+_).sortBy(_._2,ascending = false,1).foreach(e=>println("视频:"+ e._1 + " 独立IP数:" + e._2))
//    data.map(e=>((e._2,e._1),1)).filter(_._1._1 != "").reduceByKey((x,y)=>x).mapPartitions(e=>{
//      e.toList.map(_._1._1).groupBy(x=>x).map(e1=>(e1._1,e1._2.size)).toIterator
////      e.toList.map(e1=>(e1._1._1,1)).groupBy(_._1).map(e2=>(e2._1,e2._2.size)).toIterator
//    }).sortBy(_._2,ascending = false,1).foreach(e=>println("视频:"+ e._1 + " 独立IP数:" + e._2))

  }

  // 统计独立IP访问量前10位
  def ipStatics(data: RDD[(String, String, String, Long, String)]): Unit = {
    val ips: RDD[(String, Int)] = data.map(e=>(e._1,1)).reduceByKey(_+_).cache()
    val ipnum: Long = ips.count()
    val top10: Array[(String, Int)] = ips.sortBy(_._2,ascending = false).take(10)
    println("\n统计独立IP访问量前10位：")
    top10.foreach(println)
    println("独立IP数：" + ipnum)
    println()
  }

}
