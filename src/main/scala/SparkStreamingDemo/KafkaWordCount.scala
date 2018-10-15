package SparkStreamingDemo


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * @author WangLeiKai
  *         2018/10/15  19:50
  */

object KafkaWordCount {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    /**
      * kafka参数列表
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop-master:9092,hadoop-slave02:9092,hadoop-slave03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "day12_005",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //指定主题
    val topics = Array("test")

    /**
      * 指定kafka数据源
      */
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val maped: DStream[(String, String)] = stream.map(record => (record.key, record.value))


    val words = maped.flatMap(_._2.split(" "))

    val wordsAndOne = words.map((_,1))

    val reduced = wordsAndOne.reduceByKey(_+_)


    reduced.print()

    //启动程序
    ssc.start()

    //等待程序被终止
    ssc.awaitTermination()


  }
}
