package day01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/17  16:56
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_ + _)
      .sortBy(_._2,false)
      .saveAsTextFile(args(1))

    sc.stop()
  }

}
