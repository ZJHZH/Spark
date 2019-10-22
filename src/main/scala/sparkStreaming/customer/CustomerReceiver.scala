package sparkStreaming.customer

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * date 2019-10-22 11:34<br/>
 * 自定义数据源
 *
 * @author ZJHZH
 */
class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  override def onStart(): Unit = {
    new Thread("socket Receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit ={
    // 创建一个Socket
    val socket: Socket = new Socket(host,port)

    // 定义一个变量，用来接收端口传递过来的数据
    var input:String = null

    // 创建一个流对象获取数据，BufferedReader用来读取端口中的数据
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    // 读取数据
    input = reader.readLine()

    // 对receiver没有关闭，并且数据输入不为空，则发送给Spark
    while (!isStopped() && input!=null){
      // 保存数据
      store(input)

      // 继续读取
      input = reader.readLine()
    }

    // 跳出循环关闭资源
    reader.close()
    socket.close()

    // 重新连接OnStart方法
    restart("socket Receiver")
  }

  // 循环停止的时候执行
  override def onStop(): Unit = {}
}
