package ip

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/23  16:11
  */
object TestPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val file = sc.textFile("d:/data/p.txt")
    val rdd = sc.makeRDD(Array(1,2,3))
    rdd.cache()
    rdd.collect()
    println(file.partitions.size)

  }
}
