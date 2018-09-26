package access

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/26  10:32
  * 获取访问次数超过n次的IP地址
  */
object TopN1 {
  def main(args: Array[String]): Unit = {
    //程序的入口
    val conf = new SparkConf().setAppName("ContentSize").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取文件
    val lines = sc.textFile("C:\\Users\\dell\\Desktop\\access_2013_05_31.log")
    //将IP地址和1放到一个元组里
    val total: RDD[(String, Int)] = lines.filter(line => ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log: ApacheAccessLog = ApacheAccessLog.parseLogLine(line)
      (log.ipAddress,1)
    })
    //本地聚合
    val reduced: RDD[(String, Int)] = total.reduceByKey(_+_)
    //过滤掉小于a的值
    val value = reduced.filter(tuple => tuple._2 > args(0).toInt)
    //打印
    println(value.collect().toBuffer)
    //释放资源
    sc.stop()
  }
}
