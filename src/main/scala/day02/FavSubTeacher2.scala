package day02

import java.net.URL

import day01.MySpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/19  9:47
  */
object FavSubTeacher2 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)

    val lines: RDD[String] = sc.textFile("F:\\上课画图\\spark 02\\课件与代码\\teacher(1).log")
    val subjects = Array("bigdata", "javaee", "php")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher: String = line.substring(line.lastIndexOf("/") + 1)
      val host = new URL(line).getHost
      val subject = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    for (sb <- subjects){
      //这里根据课程名进行过滤
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      val sorted: RDD[((String, String), Int)] = filtered.sortBy(_._2,false)
      val array = sorted.take(2)
      println(array.toBuffer)
    }
    sc.stop()
  }
}
