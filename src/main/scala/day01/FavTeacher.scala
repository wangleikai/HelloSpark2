package day01

import java.net.URL

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/18  19:31
  * http://javaee.edu360.cn/zhaoliu
  * http://bigdata.edu360.cn/wangwu
  * http://javaee.edu360.cn/zhaoliu
  * http://javaee.edu360.cn/zhaoliu
  * http://bigdata.edu360.cn/wangwu
  */
object FavTeacher {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)
    val lines: RDD[String] = sc.textFile("D:/data/teacher.txt")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher: String = line.substring(line.lastIndexOf("/") + 1)
      val host = new URL(line).getHost
      val subject = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    val result: RDD[((String, String), Int)] = reduced.sortBy(_._2,false)
    result.take(3).foreach(println)

    sc.stop()
  }
}
