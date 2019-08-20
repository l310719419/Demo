package com.ETL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 格式转换
  */


object txt2parquet {

  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序！！！")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)
  }

}
