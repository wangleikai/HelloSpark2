package day04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/28  15:38
  */
case class User(id:Int,name : String,age:Int,fv:Int)

object SQLDemo1 {

  def main(args: Array[String])= {
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile("d://data//person.txt")
    val boyRDD: RDD[User] = lines.map(tp => {
      val fields = tp.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      User(id, name, age, fv)
    })
    import sqlContext.implicits._
    val f: DataFrame = boyRDD.toDF

    f.registerTempTable("t_boy")

    val result = sqlContext.sql("select id,name,age,fv from t_boy order by fv desc,age asc")
    result.show()
    sc.stop()
  }
}



