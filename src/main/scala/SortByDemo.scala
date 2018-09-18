import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author WangLeiKai
  *         2018/9/18  18:39
  */
object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortByDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("s",3),("w",2)))
    //sortBy方法的第二个参数的默认值是true  是升序排序
    rdd.sortBy(_._2).collect().foreach(println)
    //sortByKey的第二个参数的默认值也是true  升序 是按照hashcode进行排序
    rdd.sortByKey().collect().foreach(println)
    sc.stop()
  }
}
