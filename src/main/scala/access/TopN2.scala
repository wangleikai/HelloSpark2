package access

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/26  10:44
  */
object TopN2 {
  def main(args: Array[String]): Unit = {
    //程序的入口
    val conf = new SparkConf().setAppName("ContentSize").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //读取文件
    val lines = sc.textFile("C:\\Users\\dell\\Desktop\\access_2013_05_31.log")
    //将资源和1放到一个远足里面
    val total: RDD[(String, Int)] = lines.filter(line => ApacheAccessLog.isValidateLogLine(line)).map(line => {
      val log: ApacheAccessLog = ApacheAccessLog.parseLogLine(line)
      (log.endpoint,1)
    })
    //本地聚合
    val reduced: RDD[(String, Int)] = total.reduceByKey(_+_)
    val a = 10
    //调用自定义比较器
    val tuples = reduced.top(a)(TupleOrdering)
    //打印
    println(tuples.toBuffer)
    //释放资源
    sc.stop()

  }
}




  /**
    * 自定义的一个二元组的比较器
    */
  object TupleOrdering extends Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      // 按照出现的次数进行比较
      x._2.compare(y._2)
    }
  }


