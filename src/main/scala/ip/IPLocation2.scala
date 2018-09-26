package ip

import java.sql.DriverManager

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/22  9:32
  */
object IPLocation2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    //从hdfs读取规则文件
    val rulesLine: RDD[String] = sc.textFile("hdfs://hadoop-master:9000/data/ip/ip.txt")
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLine.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //将所又的结果收集到driver端
    val rulesInDriver = ipRulesRDD.collect()

    //发布广播变量
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)
    //读取日志文件
    val accseeLines: RDD[String] = sc.textFile("hdfs://hadoop-master:9000/data/ip/access.log")

    val provinceAndOne = accseeLines.map(log => {
      val fields = log.split("[|]")
      val ip =fields(1)
      val ipNum = TestIP.ip2Long(ip)
      //接受到driver端的广播变量的数据
      val rulesInExecutor = broadcastRef.value
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      //Driver端广播变量的引用是怎样跑到Executor中的呢？
      //Task是在Driver端生成的，广播变量的引用是伴随着Task被发送到Executor中的
      val index = TestIP.binarySearch(rulesInExecutor,ipNum)
      var province = "未知"
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    })
    val reduced = provinceAndOne.reduceByKey(_+_)
    //println(reduced.collect().toBuffer)
    //这个方法是每一条数据都要连一次数据库，浪费资源（网络通信需要大量的资源）
    /*reduced.foreach(tp => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","root")
      val pstmt = conn.prepareStatement("")
      pstmt.setString(1,tp._1)
      pstmt.setInt(2,tp._2)
      pstmt.executeUpdate()
      pstmt.close()
      conn.close()
    })*/
    //这是每个分区建立一次链接数据库（很大程度的减少了网络通信）
    reduced.foreachPartition(it => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/user?characterEncoding=UTF-8","root","root")
      val pstmt = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
      it.foreach(tp => {
        pstmt.setString(1,tp._1)
        pstmt.setInt(2,tp._2)
        pstmt.executeUpdate()
      })
      pstmt.close()
      conn.close()
    })

    sc.stop()
  }
}
