package com.alex.spark.demo1.demo4

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class Mypartitonner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]  % partitions

  }
}

object  TestPartitioner{
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitioner")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
    val data = sc.parallelize(Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6)))


    val p2: RDD[(Int, Int)] = data.partitionBy(new Mypartitonner(3))
   val value: RDD[(Int, (Int, Int))] = p2.mapPartitionsWithIndex((i,items)=>items.map((i,_)))

    value.collect().foreach(println)

  }
}
