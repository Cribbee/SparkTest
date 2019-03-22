package cn.cribbee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    //创建spark配置,一定要设置应用程序名字
    //val conf = new SparkConf().setAppName("SparkWordCount")
    //本地调试，设置local[4] 一个进程下启动4个线程
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[4]")
    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD(弹性分布式数据集)
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和1组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放
    sc.stop()

  }

}
