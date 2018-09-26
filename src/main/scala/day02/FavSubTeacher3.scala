package day02

import java.net.URL


import day01.MySpark
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 减少shuffle过程
  * @author WangLeiKai
  *         2018/9/19  14:42
  */
object FavSubTeacher3 {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySpark(this.getClass.getSimpleName)

    val lines: RDD[String] = sc.textFile("F:\\上课画图\\spark 02\\课件与代码\\teacher(1).log")
//    val subjects = Array("bigdata", "javaee", "php")
    sc.setCheckpointDir("hdfs://hadoop-master:9000/ck")
    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {
      val teacher: String = line.substring(line.lastIndexOf("/") + 1)
      val host = new URL(line).getHost
      val subject = host.substring(0, host.indexOf("."))
      ((subject, teacher), 1)
    })

    //取到所有的科目
    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()


    val sbPartitioner: SubjectPartitioner = new SubjectPartitioner(subjects)

    //reduceByKey方法  参数可以是分区器，如果没有的话  使用的是默认的
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(sbPartitioner,_+_)

    //val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    reduced.checkpoint()

    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(2).iterator
    })
    val tuples = sorted.collect()
    tuples.foreach(println)

    sc.stop()
  }
}
class SubjectPartitioner(sbs: Array[String]) extends Partitioner{

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
