package ip

import scala.io.{BufferedSource, Source}

/**
  * @author WangLeiKai
  *         2018/9/21  9:37
  */


object TestIP {

  /**
    * 将字符串的ip转换成十进制的长整形数据
    * @param ip
    * @return
    */
  def ip2Long(ip:String): Long ={
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  //1.0.8.0|1.0.15.255|16779264|16781311|亚洲|中国|广东|广州||电信|440100|China|CN|113.280637|23.125178
  /**
    * 从ip文件中找到需要的数据  开始 结束  省份
    * @param path
    * @return
    */
  def readRules(path: String): Array[(Long, Long, String)] = {
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
  def main(args: Array[String]): Unit = {
    val l: Long = ip2Long("114.114.114.114")
    val rules: Array[(Long, Long, String)] = readRules("d:/data/ip.txt")
    val index: Int = binarySearch(rules,l)
    val tp = rules(index)
    val province: String = tp._3

    println(province)
  }

}
