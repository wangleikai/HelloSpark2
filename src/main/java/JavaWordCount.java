import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author WangLeiKai
 * 2018/9/18  8:26
 */
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //程序的入口
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //从哪里读文件
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //扁平化
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //加1
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<>(word, 1);
            }
        });
        //分组聚合
        JavaPairRDD<String, Integer> reduce = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //颠倒顺序  排序的时候是按照key进行的 而key是string，所以进行排序
        JavaPairRDD<Integer, String> swaped = reduce.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) {
                return tp.swap();
            }
        });
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //再颠倒回来
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) {
                return tp.swap();
            }
        });
        //输出到文件
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();




    }
}
