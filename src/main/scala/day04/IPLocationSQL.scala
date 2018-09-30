package day04

import ip.TestIP
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author WangLeiKai
  *         2018/9/22  9:32
  */
object IPLocationSQL {
  def main(args: Array[String]): Unit = {
    //spark2.x支持  如果程序有SparkContext，则直接调用，如果没有，则创建
    val spark = SparkSession
      .builder()
      .appName("IPLocationSQL")
      .master("local[*]")
      .getOrCreate()
    //导入该对象的隐式转换
    import spark.implicits._
    //读规则文件
    val ruleLines: Dataset[String] = spark.read.textFile("d://data//ip.txt")
    //取到想要的字段，放在一个dataframe中
    val ruleDataFrame: DataFrame = ruleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum","enum","province")
    //读访问日志文件
    val accessline: Dataset[String] = spark.read.textFile("d://data/access.log")
    //取到想要的字段，放进一个dataframe
    val ipDataFrame: DataFrame = accessline.map(log => {
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = TestIP.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    //创建临时视图
    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    //执行sql语句
    val result: DataFrame = spark.sql("select province,count(*) counts from v_ips join v_rules ON (ip_num >= snum AND ip_num <= enum) group by province")
    //调用action  触发sql
    result.show()

    //释放资源
    spark.stop()

  }

}
