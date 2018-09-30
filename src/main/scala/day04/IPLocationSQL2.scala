package day04

import java.util.Properties

import ip.TestIP
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * @author WangLeiKai
  *         2018/9/22  9:32
  */
object IPLocationSQL2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    //spark2.x支持  如果程序有SparkContext，则直接调用，如果没有，则创建
    val spark = SparkSession
      .builder()
      .appName("IPLocationSQL2")
      .master("local[*]")
      .getOrCreate()
    //导入该对象的隐式转换
    import spark.implicits._
    //读规则文件
    val ruleLines: Dataset[String] = spark.read.textFile("d://data//ip.txt")

    //取到想要的字段，放在一个dataset中
    val ruleDataFrame: Dataset[(Long, Long, String)] = ruleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //读访问日志文件
    val rows = ruleDataFrame.collect()
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rows)
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
    ipDataFrame.createTempView("v_log")

    //创建并注册自定义函数
    spark.udf.register("ip2Province",(ipNum:Long) => {
      //接受到driver端的 广播变量
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据IP地址对应的十进制查找省份
      val index = TestIP.binarySearch(ipRulesInExecutor,ipNum)
      var province = "未知"
      if (index != -1){
       province = ipRulesInExecutor(index)._3
      }
      province
    })

    //执行sql语句
    val result: DataFrame = spark.sql("select ip2Province(ip_num) province,count(*) counts from v_log group by province order by counts desc")
    //调用action  触发sql
    //result.show()

 /*   result.foreachPartition(it => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/user?characterEncoding=UTF-8","root","root")
      val pstmt = conn.prepareStatement("INSERT INTO log VALUES (?, ?)")
      it.foreach(tp => {
        pstmt.setString(1,tp.getString(0))
        pstmt.setLong(2,tp.getLong(1))
        pstmt.executeUpdate()
      })
      pstmt.close()
      conn.close()
    })*/

    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","root")

    result.write.mode("append").jdbc("jdbc:mysql://localhost:3306/user?characterEncoding=UTF-8","v_log",properties)
    //释放资源
    spark.stop()

  }

}
