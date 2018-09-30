package day01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/18  17:42
  */
object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MapPartitionsDemo")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

    //将原来的数据和元素的下标进行拉链操作
    val index = rdd.zipWithIndex()
    index.collect().foreach(println)

    //作用于每一个rdd的分区。
    //传递的函数是一个迭代器
    //有几个分区，就会迭代几次
    val partiitons = rdd.mapPartitions(t => {
      t.map(_ * 10)
    })

    partiitons.collect().foreach(println)
    sc.stop()
  }
}
