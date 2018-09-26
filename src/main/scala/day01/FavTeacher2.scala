package day01

import java.net.URL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/18  19:45
  *         * http://javaee.edu360.cn/zhaoliu
  *         * http://bigdata.edu360.cn/wangwu
  *         * http://javaee.edu360.cn/zhaoliu
  *         * http://javaee.edu360.cn/zhaoliu
  *         * http://bigdata.edu360.cn/wangwu
  */
object FavTeacher2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)

    val lines: RDD[String] = sc.textFile("D:/data/teacher.txt")

    val subAndTea = lines.map(t => {
      val teacher: String = t.substring(t.lastIndexOf("/") + 1)
      val host: String = new URL(t).getHost
      val subject: String = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subAndTea.reduceByKey(_+_)
    val result = reduced.sortBy(_._2,false)

    result.take(3).foreach(println)
    sc.stop()
  }
}
