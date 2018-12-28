package com.alex.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("demo1")
      .getOrCreate()
    //隐士转换
    import  spark.implicits._
    val df: DataFrame = spark.read.json("C:\\workspace\\Spark\\SparkSql\\src\\main\\resources\\people.json")

    df.createTempView("person")
    df.show()


    //注册UDAF

    spark.udf.register("myavg",CustomerUDAF)
    spark.sql("select myavg(age) from person").show()

    spark.stop()



  }

}
