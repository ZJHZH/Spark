package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * date 2019-10-09 17:00<br>
 *
 * @author ZJHZH
 */
object Test {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6))
    val res_01: Int = rdd1.reduce(_+_)

    val res_02: Long = rdd1.count()

    val res_03: Array[Int] = rdd1.collect()

    val res_04: Array[Int] = rdd1.take(2)

    val res_05: Array[Int] = rdd1.takeOrdered(3)

    val res_06: Int = rdd1.first()

//    rdd1.saveAsTextFile("hdfs://hadoop01:9000/output/01")

    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("aa",1),("bb",2),("aa",3)))

    val res_08: collection.Map[String, Long] = rdd2.countByKey()
    val res_09: collection.Map[(String, Int), Long] = rdd2.countByValue()
    val res_10: collection.Map[Int, Long] = rdd1.countByValue()

    val res_11: RDD[(String, Int)] = rdd2.filterByRange("aa","aa")

    val rdd3: RDD[(String, Array[Int])] = sc.parallelize(Array(("aa",Array(1,2,3)),("bb",Array(2,3,4)),("cc",Array(4,5,6))))

    val res_12: RDD[(String, Int)] = rdd3.flatMapValues(e=>e)

    rdd1.foreach(e=>println(e * 2))

    rdd1.repartition(2).foreachPartition((e: Iterator[Int]) =>e.foreach(println(_)))

    val ress_13: RDD[(Int, (String, Int))] = rdd2.keyBy((e: (String, Int)) =>{(e._1+e._2).hashCode})

    val res_14: RDD[String] = rdd2.keys
    val res_15: RDD[Int] = rdd2.values

    val res_16: collection.Map[String, Int] = rdd2.collectAsMap()


  }
  def test1(): Unit ={
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]"))
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 34, 5, 6, 87, 9))
    val res_1: RDD[Int] = rdd1.map(_ * 2)

    val res_2: RDD[Int] = rdd1.filter(_ < 10)

    val rdd2: RDD[Array[Int]] = sc.makeRDD(Array(Array(1, 2, 3), Array(4, 5, 6), Array(7, 8, 9)))
    val res_3: RDD[Int] = rdd2.flatMap(e => e)
    val res_4: RDD[Int] = rdd2.flatMap(_.map(_ * 2))

    val res_5: RDD[Int] = rdd1.sample(withReplacement = false,0.6)

    val rdd3: RDD[Int] = sc.makeRDD(Array(3,44,7,6,8,90,8,7,6,5))
    val res_6: RDD[Int] = rdd1.union(rdd3)
    val res_7: RDD[Int] = rdd1.intersection(rdd3)

    val res_8: RDD[Int] = rdd3.distinct()

    val rdd4: RDD[(String, Int)] = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
    val rdd5: RDD[(String, Int)] = sc.parallelize(List(("jerry",2),("tom",2),("dog",10)))

    val res_9: RDD[(String, (Int, Int))] = rdd4.join(rdd5)

    val res_10: RDD[(String, (Int, Option[Int]))] = rdd4.leftOuterJoin(rdd5)
    val res_11: RDD[(String, (Option[Int], Int))] = rdd4.rightOuterJoin(rdd5)

    val rdd6: RDD[((String, Int), (String, Int))] = rdd4.cartesian(rdd5)

    val res_12: RDD[((String, Int), Iterable[((String, Int), (String, Int))])] = rdd6.groupBy(_._2)
    val res_13: RDD[((String, Int), Iterable[(String, Int)])] = rdd6.groupByKey()

    val res_14: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd4.cogroup(rdd5)

    val rdd7: RDD[Int] =sc.parallelize(List(1,2,3,4,5,6),3)

    val res_15: RDD[Int] = rdd7.mapPartitions((_: Iterator[Int]).map((_: Int)*2))
    val res_16: RDD[(Int, Int)] = rdd7.mapPartitionsWithIndex((index: Int, iter: Iterator[Int])=>{iter.map((e: Int) =>(index,e))})

    val rdd8: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    val res_17: RDD[(Int, String)] = rdd8.sortByKey()

    val res_18: RDD[(Int, String)] = rdd8.sortBy(_._2)

    val rdd9: RDD[(String, Int)] = sc.parallelize(Array(("tom",1),("jerry",3),("kitty",2),("jerry",2)))

    val res_19: RDD[(String, Int)] = rdd9.reduceByKey(_+_)

    val res_20: RDD[(String, Int)] = rdd9.repartition(3)
    val res_21: RDD[(String, Int)] = rdd9.coalesce(3)
    val res_22: RDD[(String, Int)] = rdd9.repartitionAndSortWithinPartitions(new HashPartitioner(3))

    val res_23: RDD[(String, Int)] = res_22.aggregateByKey(10)(_+_,_+_)

    val res_24: RDD[(String, Int)] = res_20.combineByKey(e =>e, (a, b)=>a+b, (m, n)=>m+n)
    val res_25: RDD[(String, Int)] = res_20.combineByKey(_.toInt,(_: Int)+_,(_: Int)+(_: Int))
    val res_26: RDD[(String, Int)] = res_20.combineByKey[Int]((e: Int) => e,(_: Int)+(_: Int),(_: Int)+(_: Int))

    println(res_25.collect().toBuffer)
  }
}
