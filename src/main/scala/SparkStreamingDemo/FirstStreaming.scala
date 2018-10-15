package SparkStreamingDemo

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author WangLeiKai
  *         2018/10/15  19:50
  */
object FirstStreaming {
  def main(args: Array[String]): Unit = {
    //配置   SparkStreaming会有两个进程  一个是接收  一个是计算
    val conf = new SparkConf().setAppName("FirstStreaming").setMaster("local[*]")

    val context = new SparkContext(conf)

    //是对SparkContext的包装  增加了实时计算的功能
    val ssc: StreamingContext = new StreamingContext(context,Milliseconds(5000))

    //是从虚拟机发送socket  然后StreamingContext接收
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop-master",8888)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    reduced.print()

    //启动Spark Streaming程序
    ssc.start()
    //等待优雅的退出   不会直接杀死
    ssc.awaitTermination()
  }
}
