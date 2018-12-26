package com.alex.spark.demo1.demo3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top3 {

  def main(args: Array[String]): Unit = {


    //创建一个conf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("top3")

    //创建一个sc
    val sc = new SparkContext(conf)

    //读入文件
    val line: RDD[String] = sc.textFile("D:\\HadoopCluster\\Hadoopdata\\topn\\agent.log")

    //把文件切分((4,29),1)  按照(省,广告id)为key, 点击次数为val
    val prinviceAndADAndone: RDD[((String, String), Int)] = line.map(x => {
      val fields: Array[String] = x.split("\\s+")

      ((fields(1), fields(4)), 1)


    })

    //按照key分组,((2,22),CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1))
    val prinviceAdGroupbykey: RDD[((String, String), Iterable[Int])] = prinviceAndADAndone.groupByKey()

    //((1,25),15) 把分组后的数据进行统计次数
    val prinviceAdCount: RDD[((String, String), Int)] = prinviceAdGroupbykey.mapValues(_.size)
    val prinviceAdCount2: RDD[((String, String), Int)] = prinviceAndADAndone.reduceByKey(_+_)

    //(2,(22,15))  方便安装key分区groupByKey
    val priniceAdMap: RDD[(String, (String, Int))] = prinviceAdCount.map(x=>(x._1._1,(x._1._2,x._2)))

    //新的数据集4,CompactBuffer((12,25), (25,11), (27,13), (13,21), (7,7), (6,15), (1,20), (8,21), (3,15), (22,18), (19,10), (10,10),
    val prinviceAdMapGroupagin: RDD[(String, Iterable[(String, Int)])] = priniceAdMap.groupByKey()

//(4,List((12,25), (16,22), (2,22)))  最终结果  ,使用sortby的方式, 没有降序,排序完成后再reverse
    val res: RDD[(String, List[(String, Int)])] = prinviceAdMapGroupagin.mapValues(_.toList.sortBy(_._2).reverse.take(3))
//使用sortwith的方法排序
    val provinceAdTop3 = prinviceAdMapGroupagin.mapValues { x =>
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    }



    res.foreach(println(_))

  }


}
