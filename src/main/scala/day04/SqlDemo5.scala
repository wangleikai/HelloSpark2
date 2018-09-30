package day04

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author WangLeiKai
  *         2018/9/29  14:41
  *
  * json数据读取
  * 不用写StructType  直接对应
  * 读取json数据的两种格式
  * SQL和DSL的不同
  */
object SqlDemo5 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("SqlDemo5")
      .master("local[*]")
      .getOrCreate()

    //val lines: DataFrame = session.read.json("d://data//emp.json")
    val lines = session.read.format("json").load("d://data//emp.json")

    val frame = lines.select("empno","ename","job")
    //lines.createTempView("v_json")
    //val frame: DataFrame = session.sql("select * from v_json where deptno = 10 and sal > 1500")

    frame.show()

    session.stop()
  }
}
