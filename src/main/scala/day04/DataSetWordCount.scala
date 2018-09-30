package day04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author WangLeiKai
  *         2018/9/28  19:33
  */
object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DataSetWordCount")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("d://data/p.txt")


    import spark.implicits._

    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //val result = words.groupBy($"value" as "word").count().sort($"count" desc)
    import org.apache.spark.sql.functions._
    val result = words.groupBy($"value" as "word").agg(count("*") as "count").sort($"count" desc)
    result.show()
    spark.stop()
  }
}
