package com.ETL.Rept

import com.ETL.utils.RptUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TerNet {

  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if(args.length != 2){
      println("路径出错，退出程序！！！")
      sys.exit()
    }

    //创建一个集合保存输入输出路径
    val Array(inputPath,outputPath) = args

    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //读取文件
    val df = sqlContext.read.parquet(inputPath)

    //定义一个方法把需要的字段全部取出来

    df.map( row =>{
      val requestmode = row.getAs[Int]("requestmode")

      val processnode = row.getAs[Int]("processnode")

      val iseffective = row.getAs[Int]("iseffective")

      val isbilling = row.getAs[Int]("isbilling")

      val isbid = row.getAs[Int]("isbid")

      val iswin = row.getAs[Int]("iswin")

      val adorderid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")

      val adpayment = row.getAs[Double]("adpayment")

      //Key值  是网络名称
      val networkmannername = row.getAs[String]("networkmannername")
      //写一个方法对运营商名称进行处理：

      //写三个方法处理九个指标
      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (networkmannername, reqlist ++ clicklist ++ adlist)
    })
      //根据Key聚合Value
      .reduceByKey(
      (List1, List2) => {
        List1.zip(List2).map(t => t._1+ t._2)
      })
      //整理元素
      .map(t =>{
      t._1+","+t._2.mkString(",")
    })
      .saveAsTextFile(outputPath)

    sc.stop()
  }

}
