package sparkStreaming.SildingDemo

/**
 * date 2019-10-22 16:00<br/>
 * 纯Scala代码
 * @author ZJHZH
 */
object Demo {
  def main(args: Array[String]): Unit = {
    val list: List[Int] = List(1,2,3,4,5,6)

    // 函数，滑动
    val iterator: Iterator[List[Int]] = list.sliding(2)

    for (elem <- iterator) {
      println(elem.mkString(","))
    }
  }
}
