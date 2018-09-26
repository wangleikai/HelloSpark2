package day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/25  21:24
  */
object CustomSort2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort1").setMaster("local[*]")
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
    //排序的时候传入一个规则，不会改变数据的格式，只会改变数据的顺序
    val sorted: RDD[(String, Int, Int)] = userRdd.sortBy(tp => new Boy(tp._2,tp._3))
    val r = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

//实现序列化的原因是实体要在网络之间进行传递   本地模拟集群运行
//规则也要shuffle  所以也得实现序列化
class Boy(val age: Int, val fv: Int) extends Ordered[Boy] with Serializable {
  override def compare(that: Boy): Int ={
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }
}