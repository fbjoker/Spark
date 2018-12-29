package com.alex.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}


object demo1 {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("demo1")
      .getOrCreate()
    //隐士转换
    import  spark.implicits._
    val df: DataFrame = spark.read.json("C:\\workspace\\Spark\\SparkSql\\src\\main\\resources\\people.json")



    df.filter($"age">20).show(100)
     new Properties()

    df.createTempView("person")

    spark.sql("select * from person").show()

    spark.stop()



  }

}
