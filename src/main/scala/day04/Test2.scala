package day04

/**
 * date 2019-10-12 14:27<br>
 *
 * @author ZJHZH
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    println("[15/Feb/2017:11:00:46".replaceFirst("\\[\\d+/\\w+/\\d+:(\\d+):.*", "$1"))
    println("[15/Feb/2017:11:00:46".substring(13, 15))
    println("http://cdn.v.abc.com.cn/141011.mp4/aaa".replaceFirst("http://cdn.v.abc.com.cn/([\\w]+\\.mp4).*", "$1"))
    println("http://cdn.v.abc.com.cn/videojs/video.js".replaceFirst("http://cdn.v.abc.com.cn/([\\w]+\\.mp4)", "$1"))
    val str: String = "https://v.abc.com.cn/video/iframe/player.html?id=141081&autoPlay=1%20%0A%0A".replaceFirst(".*\\?id=(\\w+?)&.*","$1.mp4")
    println(str)
  }

  def binarySearch(arr: Array[(String, String, String)], ip: String): Int = {
    var start: Int = 0
    var end: Int = 0
    while (start < end) {
      val middle: Int = (start + end) / 2
      if (ip >= arr(middle)._1 && ip <= arr(middle)._2){
        return middle
      }
      if (ip < arr(middle)._1){
        end -= 1
      }
      if (ip > arr(middle)._2){
        start += 1
      }
    }
    return -1
  }
}
