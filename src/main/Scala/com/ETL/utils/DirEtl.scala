package com.ETL.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DirEtl {

  def main(args: Array[String]): Unit = {


    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    //创建入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //读取文件
    val log = sc.textFile("D:\\jietu\\app_dict.txt")

    //对数据进行清洗
    val info=log.map(_.split("\\s"))

    val data = info.map(x => {
      val name = x(4)
      val id = x(1)
    }).collect().toBuffer


    sc.stop()

  }


}