package day04

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author WangLeiKai
  *         2018/10/2  21:42
  */
object SQLFavSubTeacher {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLFavSubTeacher").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("F:\\上课画图\\spark 02\\课件与代码\\teacher(1).log")

    import spark.implicits._

    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)
      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")

    val sql1 = spark.sql("select subject,teacher,count(*) counts from v_sub_teacher group by subject,teacher")

    sql1.createTempView("v_temp_sub_teacher_counts")

    val sql2 = spark.sql("SELECT *, dense_rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= 2")

    sql2.show()

    spark.stop()
  }
}
