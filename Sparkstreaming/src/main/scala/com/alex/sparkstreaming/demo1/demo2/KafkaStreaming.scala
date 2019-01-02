package com.alex.sparkstreaming.demo1.demo2


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaStreaming {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")


    val sc = new StreamingContext(conf,Seconds(5))


    //kafka参数声明
    val brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //kafka参数

    val kafakapara = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,

    )


   //KafkaUtils.createDirectStream(sc,kafakapara,)
  }

}
