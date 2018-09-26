package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/25  21:24
  */
object CustomSort5 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort5").setMaster("local[*]")
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
    //Ordering[(Int,Int)] 最终比较的规格格式
    //on[(String,Int,Int)]未比较之前的数据格式
    //(t => (-t._3,t._2))怎样将规则转换成想要比较的格式

    implicit val rules = Ordering[(Int,Int)].on[(String,Int,Int)](t => (-t._3,t._2))
    val sorted: RDD[(String, Int, Int)] = userRdd.sortBy(tp =>tp)
    val r = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

