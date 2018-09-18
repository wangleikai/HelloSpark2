import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/18  17:54
  */
object MapPartitionsWithIndexDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MapPartitionsWithIndexDemo")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(12,3,4,5,6,4),2)

    // 定义一个函数，返回rdd中的数据，以及对应的分区编号
    val f=(i:Int,it:Iterator[Int])=> {
      it.map(t=> s"p=$i,v=$t")
    }

    val rdd2 = rdd.mapPartitionsWithIndex(f)
    rdd2.collect().toBuffer.foreach(println)
    sc.stop()

  }
}
