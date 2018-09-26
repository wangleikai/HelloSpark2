package day02

import java.net.URL

import day01.MySpark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author WangLeiKai
  *         2018/9/19  8:36
  * 根据学科取得最受欢迎的老师的前三名
  *
  */
object FavSubTeacher {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)

    val lines: RDD[String] = sc.textFile("F:\\上课画图\\spark 02\\课件与代码\\teacher(1).log")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher: String = line.substring(line.lastIndexOf("/") + 1)
      val host = new URL(line).getHost
      val subject = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    //使用toList将所有的数据放在内存中进行排序  会产生内存溢出
    //不能排序的原因  迭代器没有排序方法
    val result: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(2))

    println(result.collect().toBuffer)
    sc.stop()



  }
}
