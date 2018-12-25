package com.alex.spark.demo1.demo2

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("wc2").setMaster("local[*]")
    val sc = new SparkContext(conf)


   // sc.textFile(args(0)).flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).saveAsTextFile(args(1))

    println(sc.parallelize(1 to 10).partitions.length)


  }

}
