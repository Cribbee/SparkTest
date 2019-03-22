package cn.cribbee.spark;

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
 * ClassName: JavaWordCount
 * Description: TODO
 * Author: Cribbee
 * Date: 2019/3/14、4:56 PM
 * Version: 1.0
 **/
public class JavaWordCount {

    //psvm出来
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWorldCount");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据 jsc.textFile(args[0]).var 直接出来命名变量
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            //要求返回迭代器，所以不可以直接返回line.split（" "），需要用到Arrays.asList 把数组变为集合
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        //将单词和1组合在一起  原来是单词，现在是要一对了（元组）所以需要mtP
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        //聚合 两两结合再跟其他相加，I1=1一个相同key的value，I2=1一个相同key的value，I3=相加后返回值的类型
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //调换顺序，把次数放前面，单词放后面，进行排序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tp) throws Exception {
                //return new Tuple2<>(tp._2, tp._1);
                return tp.swap();
            }
        });

        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);

        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tp) throws Exception {
                return tp.swap();
            }
        });

        //将数据保存到hdfs
        result.saveAsTextFile(args[1]);

        //释放资源
        jsc.stop();


    }
}
