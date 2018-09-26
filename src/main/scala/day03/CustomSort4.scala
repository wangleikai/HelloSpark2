package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/25  21:24
  */
object CustomSort4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort4").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    
    val lines = sc.parallelize(users)

    val userRdd = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      //new User(name, age, fv)
      (name,age,fv)
    })
    //val sorted = userRdd.sortBy(u => u)
    //充分利用元组的排序规则
    val sorted: RDD[(String, Int, Int)] = userRdd.sortBy(tp =>(-tp._3,tp._2))
    val r = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

