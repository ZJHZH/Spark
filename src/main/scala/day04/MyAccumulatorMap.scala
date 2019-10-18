package day04

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * date 2019-10-12 11:36<br>
 *
 * @author ZJHZH
 */
class MyAccumulatorMap extends AccumulatorV2[String,mutable.HashMap[String, Int]]{
    private val stringToInt: mutable.HashMap[String, Int] = scala.collection.mutable.HashMap[String,Int]()

    override def isZero: Boolean = stringToInt.isEmpty

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = this

    override def reset(): Unit = stringToInt.clear()

    override def add(v: String): Unit = {
      add(v,1)
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
      other.value.foreach(e=>add(e._1,e._2))
    }

    override def value: mutable.HashMap[String, Int] = stringToInt

    private[this] def add(k: String, v: Int):Unit = {
      if (stringToInt.contains(k)){
        stringToInt(k) += v
      }else{
        stringToInt(k) = v
      }
    }







//  private val stringToInt: mutable.HashMap[String, Int] = scala.collection.mutable.HashMap[String,Int]()
//
//  override def isZero: Boolean = stringToInt.isEmpty
//
//  override def copy(): AccumulatorV2[String, Array[(String, Int)]] = this
//
//  override def reset(): Unit = stringToInt.clear()
//
//  override def add(v: String): Unit = {
//    add(v,1)
//  }
//
//  override def merge(other: AccumulatorV2[String, Array[(String, Int)]]): Unit = {
//    other.value.foreach(e=>add(e._1,e._2))
//  }
//
//  override def value: Array[(String, Int)] = stringToInt.toArray
//
//  private[this] def add(k: String, v: Int):Unit = {
//    if (stringToInt.contains(k)){
//      stringToInt(k) += v
//    }else{
//      stringToInt(k) = v
//    }
//  }
}
