import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author WangLeiKai
 * 2018/9/18  9:21
 */
public class LambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LambdaWordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile(args[0]);

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((x, y) -> x + y);

        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());

        JavaPairRDD<String, Integer> result = swaped.mapToPair(tp -> tp.swap());

        result.saveAsTextFile(args[1]);

        jsc.stop();

    }
}
