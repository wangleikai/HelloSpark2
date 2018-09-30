package day04

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author WangLeiKai
  *         2018/9/28  15:38
  */
case class User2(id:Int,name : String,age:Int,fv:Int)

object SQLDemo4 {

  def main(args: Array[String])= {
    val spark = SparkSession
      .builder()
      .appName("SQLDemo4")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val lines = spark.read.textFile("d://data//person.txt")
    val dst: Dataset[User] = lines.map(tp => {
      val fields = tp.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      User(id, name, age, fv)
    })

    dst.createTempView("t_boy")



    val result = spark.sql("select id,name,age,fv from t_boy order by fv desc,age asc")
    result.show()
    spark.stop()
  }
}



