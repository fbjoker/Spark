package com.alex.spark.demo1.demo4

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRdd {

  def main(args: Array[String]): Unit = {

    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    sc.parallelize(List((1,3),(1,2),(2,4),(2,3),(3,6),(3,8),(3,5),(3,99),(3,555)),8)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/ct"
    val userName = "root"
    val passWd = "root123"

    val sqlrdd = new JdbcRDD[(Int, String)](
      sc,
      () => {
        Class.forName(driver)

        DriverManager.getConnection(url, userName, passWd)
      },
      "select * from ct_user where id>? and id<?",
      1,
      10,
      2,
      res => (res.getInt(1), res.getString(2))


    )
    sqlrdd.foreach(println)



  }

}
