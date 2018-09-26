package day01

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/18  18:51
  */
object MySpark {
  // 获取local模式下的sc
  def apply(appName: String) = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
      .setAppName(appName)
    new SparkContext(conf)
  }
}
