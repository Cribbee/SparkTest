package cn.cribbee.spark.scala

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import scala.collection.mutable.ListBuffer

object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val topN = args(1).toInt
    val subjects = Array("bigdata", "javaee", "php")

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

    val result = new ListBuffer[((String, String), Int)]
    //scala的集合排序是在内存中进行的，但是内存有可能不够用
    //可以调用RDD的sortBy方法，内存+磁盘进行排序
    for(sb <- subjects){
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      //现在调用RDD的sortBy方法，（take是一个action，会触发任务提交）
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(topN)

      //
      result ++= favTeacher.toList

      //打印
      println(result)
    }



      sc.stop()

  }

}
