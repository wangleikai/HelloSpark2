package day04

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * @author WangLeiKai
  *         2018/9/28  20:37
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()
    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laowang,china","2,laoli,usa"))

    val tpDs = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nation = fields(2)
      (id, name, nation)
    })
    val df1 = tpDs.toDF("id","name","nation")
    val nat: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))

    val nations = nat.map(l => {
      val fields = l.split(",")
      val cname = fields(0)
      val ename = fields(1)
      (ename, cname)
    })
    val df2 = nations.toDF("cname","ename")

    df1.createTempView("v_users")
    df2.createTempView("v_nations")

    val result = spark.sql("SELECT name, cname FROM v_users JOIN v_nations ON nation = ename")
    result.show()
    spark.stop()
  }
}
