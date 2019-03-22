package cn.cribbee.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * ClassName: JavaLambdaWordCount
 * Description: TODO
 * Author: Cribbee
 * Date: 2019/3/17、9:21 PM
 * Version: 1.0
 **/
public class JavaLambdaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWorldCount");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据 jsc.textFile(args[0]).var 直接出来命名变量
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        //将单词和1组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));

        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);

        //调整顺讯
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());

        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());

        //保存结果
        result.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();
    }
}
