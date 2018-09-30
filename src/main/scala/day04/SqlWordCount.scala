package day04

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author WangLeiKai
  *         2018/9/28  19:33
  */
object SqlWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SqlWordCount")
      .master("local[*]")
      .getOrCreate()



    val lines = spark.read.textFile("d://data/p.txt")

    import spark.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    words.createTempView("v_wc")

    val result: DataFrame = spark.sql("select value,count(*) counts from v_wc group by value order by counts")
    result.show()
    spark.stop()
  }
}
