package com.alex.spark.Demo1a

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql1a {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession
      .builder()
      .appName("slq1")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import  spark.implicits._

    val df: DataFrame = spark.read.json("C:\\workspace\\Spark\\SparkSql\\src\\main\\resources\\people.json")

    df.createTempView("p")
    val df2: DataFrame = spark.sql("select * from p")

     val properties = new Properties()

    properties.put("user","root")
    properties.put("password","root123")

    val dfsql: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop102:3306/ct?characterEncoding=utf-8","ct_user",properties)

    dfsql.write.mode(SaveMode.Append ).jdbc("jdbc:mysql://hadoop102:3306/ct?characterEncoding=utf-8","ct_user_bk",properties)


   spark.sql("show tables").show


    //dfsql.show()






    spark.stop()




  }

}
