package day04

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * date 2019-10-12 11:14<br>
 *
 * @author ZJHZH
 */
class MyAccumulatorSet extends AccumulatorV2[String,mutable.Set[String]]{
  private val set: mutable.Set[String] = mutable.Set[String]()

  override def isZero: Boolean = set.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Set[String]] = this

  override def reset(): Unit = set.clear()

  override def add(v: String): Unit = set.add(v)

  override def merge(other: AccumulatorV2[String, mutable.Set[String]]): Unit = this.set.union(other.value)

  override def value: mutable.Set[String] = set
}
