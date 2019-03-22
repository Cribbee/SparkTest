package cn.cribbee.spark.scala

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner

import scala.collection.mutable


object GroupFavTeacher3 {
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

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    //自定义一个分区器，并且按照指定的分区器进行分区
    val sbPartitioner = new SubjectPartitioner(subjects)
    //partitionBy按照指定的分区规则进行分区
    //调用partitionBy的RDD的Key是(String, String)
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    //现在一个分区上只有一个学科，如果一次拿出一个分区（可以操作一个分区中的数据了，一个分区就是一个迭代器）
    val sorted = partitioned.mapPartitions(it => {
      //将迭代器转换成List，然后排序再转换成迭代器返回，因为源码要求返回迭代器类型
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })
    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)
    sc.stop()

  }

}

//自定义分区器
class SubjectPartitioner(sbs: Array[String]) extends Partitioner{

  //相当于主构造器（new的时候会执行一次）
  //用来存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  //传入一个学科对应一个编号
  var i = 0
  for (sb <- sbs){
    //rules(sb) = i
    rules.put(sb, i)
    i += 1

  }

  //返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length

  //根据传入的key计算分区的标号
  //key是一个元组(String, String)
  override def getPartition(key: Any): Int = {
    //获得学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    //根据规则计算分区编号
    rules(subject)
  }
}