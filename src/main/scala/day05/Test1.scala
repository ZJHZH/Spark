package day05

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.{SparkConf, SparkContext}

/**
 * date 2019-10-14 9:05<br>
 *
 * @author ZJHZH
 */
object Test1 {
  //  val conf: SparkConf = new SparkConf().setAppName("Test1").setMaster("local[*]")
  //  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    println("qweasd&as.dqew&qweq.cq?aqwes.cqwe".replaceAll(".*?[&?](\\w+\\.c).*", "$1"))

    val str: String = "https://v-cdn.abc.com.cn/140819.mp4"
    val pattern: Pattern = Pattern.compile(".*/([\\w]+\\.mp4).*")
    val matcher: Matcher = pattern.matcher(str)
    if (matcher.matches()) {
      println(matcher.group(1))
    }

  }

  def test1(): Unit = {
    val str: String = "https://v-cdn.abc.com.cn/140819.mp4"
    val pattern: Pattern = Pattern.compile(".*?/([\\w]+\\.mp4).*")
    val l1: Long = System.currentTimeMillis()
    for (i <- 1 to 100000000) {
      str.replaceFirst(".*?/([\\w]+\\.mp4).*", "$1")
    }
    val l2: Long = System.currentTimeMillis()

    for (i <- 1 to 100000000) {
      pattern.matcher(str).replaceFirst("$1")
    }
    val l3: Long = System.currentTimeMillis()

    println(l2 - l1) //79029
    println(l3 - l2) //39234
  }
}
