package com.ETL.Rept

import com.ETL.utils.RptUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Terop {

  def main(args: Array[String]): Unit = {

    //判断卢静是否存在
    if(args.length != 2){
      println("地址输入错误，请检查！！！")
      sys.exit()
    }

    //创建一个集合存放输入输出目录
    val Array(inputPath,outputPath)=args

    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")

      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //获取数据
    val df = sqlContext.read.parquet(inputPath)

    //自定义方法取出所有所需字段
    df.map(row =>{
      val requestmode = row.getAs[Int]("requestmode")

      val processnode = row.getAs[Int]("processnode")

      val iseffective = row.getAs[Int]("iseffective")

      val isbilling = row.getAs[Int]("isbilling")

      val isbid = row.getAs[Int]("isbid")

      val iswin = row.getAs[Int]("iswin")

      val adorderid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")

      val adpayment = row.getAs[Double]("adpayment")

      //Key值  是运营商名称
      val ispname = row.getAs[String]("ispname")
      //写一个方法对运营商名称进行处理：

      //写三个方法处理九个指标
      val reqlist: List[Double] = RptUtils.request(requestmode, processnode)
      val clicklist: List[Double] = RptUtils.click(requestmode, iseffective)
      val adlist: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (ispname, reqlist ++ clicklist ++ adlist)
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
