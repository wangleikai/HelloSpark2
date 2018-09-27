package day03

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/26  14:43
  */
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val getConn =() => DriverManager.getConnection("jdbc:mysql://localhost:3306/user?characterEncoding=UTF-8","root","root")
    val rdd = new JdbcRDD(
      //注意  不能使用小于或者大于   会将分区中的最后一个元素删除
      sc, getConn, "select * from access_log where id >= ? and id <= ?", 1, 5, 2,
      rs => {
      val province = rs.getString(2)
      val num = rs.getInt(3)
      (province,num)
    }
    )
    val r = rdd.collect()
    println(r.toBuffer)
    sc.stop()

  }
}
