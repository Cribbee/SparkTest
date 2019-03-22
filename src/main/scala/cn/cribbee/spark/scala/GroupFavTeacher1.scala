package cn.cribbee.spark.scala

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val topN = args(1).toInt

    //指定以后从哪里读数据
    val lines: RDD[String] = sc.textFile(args(0))
    //整理数据
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index: Int = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher),1)
    })

    //聚合，将学科和老师联合起来当做Key
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)

    //reduced.groupBy((f: ((String, String), Int)) => f._1._1, 4) 如果要执行groupBy的时候想修改分区数量，需要指定函数类型
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    //经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    //将每一个组拿出来进行操作
    //为什么可以调用scala的sortBy方法？ 因为一个学科的数据已经在一台机器上的一个scala集合里面了，toList写入内存，reverse反转为降序
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))

    //收集记过
    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    //打印
    println(result.toBuffer)


    sc.stop()

  }

}
