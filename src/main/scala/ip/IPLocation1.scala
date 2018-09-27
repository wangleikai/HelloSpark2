package ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/22  9:32
  */
object IPLocation1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation1").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    //读取规则文件
    val rules = TestIP.readRules(args(0))
    //发布广播变量
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)
    //读取日志文件
    val accseeLines: RDD[String] = sc.textFile(args(1))

    val func = ( line:String) => {
      val fields = line.split("[|]")
      val ip =fields(1)
      val ipNum = TestIP.ip2Long(ip)
      //接受到driver端的广播变量的数据
      //收集到的数据是放在内存中的
      val rulesInExecutor = broadcastRef.value
      val index = TestIP.binarySearch(rulesInExecutor,ipNum)
      var province = "未知"
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    }

    val provinceAndOne = accseeLines.map(func)
    val reduced = provinceAndOne.reduceByKey(_+_)

    val r = reduced.collect().toBuffer
    println(r)
    sc.stop()




  }

}
