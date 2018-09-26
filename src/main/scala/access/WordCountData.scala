package access

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/26  10:28
  * 统计各个不同返回值出现的数据个数
  */
object WordCountData {
  def main(args: Array[String]): Unit = {
      //程序的入口
      val conf = new SparkConf().setAppName("ContentSize").setMaster("local[*]")
      val sc = new SparkContext(conf)
      //读取文件
      val lines = sc.textFile("C:\\Users\\dell\\Desktop\\access_2013_05_31.log")
      //将返回值和1放到一个元组里
      val total: RDD[(Int, Int)] = lines.filter(line => ApacheAccessLog.isValidateLogLine(line)).map(line => {
        val log: ApacheAccessLog = ApacheAccessLog.parseLogLine(line)
        (log.responseCode,1)
      })
      //聚合
      val reduced: RDD[(Int, Int)] = total.reduceByKey(_+_)
      //打印
      println(reduced.collect().toBuffer)
      //释放资源
      sc.stop()
  }
}
