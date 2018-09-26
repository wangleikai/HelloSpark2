package day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/25  21:24
  */
object CustomSort1 {
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
      new User(name, age, fv)
    })
    val sorted = userRdd.sortBy(u => u)
    val r = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

//实现序列化的原因是实体要在网络之间进行传递   本地模拟集群运行
class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int ={
    if(this.fv == that.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = s"name: $name, age: $age, fv: $fv"
}