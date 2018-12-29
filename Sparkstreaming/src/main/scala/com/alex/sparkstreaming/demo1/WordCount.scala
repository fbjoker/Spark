package com.alex.sparkstreaming.demo1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WC")

    val sc = new StreamingContext(conf,Seconds(5))


    val dst: ReceiverInputDStream[String] = sc.socketTextStream("hadoop102",9999)


    val wordDst: DStream[String] = dst.flatMap(_.split("\\s+"))

    val wordAndOne: DStream[(String, Int)] = wordDst.map((_,1))
    val wordCount: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)


    wordCount.print()


    //这里不能关闭资源, 开启流式计算

    sc.start()

    sc.awaitTermination()




  }

}
