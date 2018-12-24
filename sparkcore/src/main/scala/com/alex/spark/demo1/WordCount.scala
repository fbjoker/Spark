package com.alex.spark.demo1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {


    //1 new conf
    //本地模式需要加上local
    //val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val conf: SparkConf = new SparkConf().setAppName("wc")
    //2 new sc
    val sc = new SparkContext(conf)


    //3 wordcount

   sc.textFile(args(0)).flatMap(_.split("\\s+"))
     .map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))





    //4.关闭sc
    sc.stop()

  }

}
