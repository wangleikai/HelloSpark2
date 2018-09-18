import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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



    }
}
