package access

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/26  9:03
  * 求contentSize的平均值 最大值 最小值
  */
object ContentSize {
  def main(args: Array[String]) = {
    //程序的入口
    val conf = new SparkConf().setAppName("ContentSize").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取文件
    val lines = sc.textFile("C:\\Users\\dell\\Desktop\\access_2013_05_31.log")
    //取到返回结果的数据
    val total: RDD[Long] = lines.filter(line => ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log: ApacheAccessLog = ApacheAccessLog.parseLogLine(line)
      log.contentSize
    })
    //直接调用RDD的方法
    //总和
    val zonghe= total.sum()
    //出现的次数
    val count: Long = total.count()
    //平均值
    val avg = zonghe/count
    //最大值
    val maxx: Long = total.max()
    //最小值
    val minn: Long = total.min()
    //打印
    println(s"pingjunzhi is $avg, zuidazhi is $maxx,zuixiaozhi is $minn")
    //释放资源
    sc.stop()
  }

}
