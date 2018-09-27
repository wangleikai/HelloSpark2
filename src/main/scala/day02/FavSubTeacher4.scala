package day02

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author WangLeiKai
  *         2018/9/27  18:53
  */
object FavSubTeacher4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FavSubTeacher4").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("F:\\上课画图\\spark 02\\课件与代码\\teacher(1).log")

    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher: String = line.substring(line.lastIndexOf("/") + 1)
      val host = new URL(line).getHost
      val subject = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })
    //取到所有的科目
    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    val sbPartitioner: SubjectPartitioner2 = new SubjectPartitioner2(subjects)

    //reduceByKey方法  参数可以是分区器，如果没有的话  使用的是默认的
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(sbPartitioner,_+_)

    val mapped: RDD[(String, (String, Int))] = reduced.map(tp => {
      val sub = tp._1._1
      val name = tp._1._2
      val num = tp._2
      (sub, (name, num))
    })


    val grouped: RDD[(String, Iterable[(String, Int)])] = mapped.groupByKey()
    val retRDD:RDD[(String, Iterable[(String, Int)])] = grouped.map(tuple => {
      var ts = new mutable.TreeSet[(String, Int)]()(new Ordering[(String, Int)]{
        override def compare(x: (String, Int), y: (String, Int)): Int = {

          val xField = x._2.toInt
          val yField = y._2.toInt
          -(xField - yField)
        }
      })
      val subject = tuple._1
      val nameNums = tuple._2
      for(nameNum <- nameNums) {
        // 添加到treeSet中
        ts.add(nameNum)
        if(ts.size > 2) {
          ts = ts.dropRight(1)
        }
      }
      (subject, ts)
    })



/*   class MyOrdering extends Ordering[(String, Int)]{
      override def compare(x: (String, Int), y: (String, Int)): Int = {

        val xField = x._2.toInt
        val yField = y._2.toInt
        xField - yField
      }
    }*/

    val tuples = retRDD.collect()
    tuples.foreach(println)

    sc.stop()
  }
}
class SubjectPartitioner2(sbs: Array[String]) extends Partitioner{

  //map里放的是科目和对应的分区号 0  1 2
  private val rules: mutable.HashMap[String, Int] = new mutable.HashMap[String,Int]()
  var index = 0
  for(sb <- sbs){
    rules.put(sb,index)
    index += 1
  }

  //返回分区的数量  下一个RDD有多少个分区
  override def numPartitions: Int = sbs.length

  //这里的key是一个元组
  override def getPartition(key: Any): Int = {

    //获取学科名称
    val subject: String = key.asInstanceOf[(String,String)]._1
    //根据规则计算分区编号
    rules(subject)
  }


}
