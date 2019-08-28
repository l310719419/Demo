package com.ETL.Mysql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object MysqlTerNet {

  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if (args.length != 2) {
      println("路径错误，请检查后重新输入！！！")
      sys.exit()
    }

    //创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")

      //设置序列化方式，采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //创建执行入口
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //读取文件
    val df = sqlContext.read.parquet(inputPath)

    //注册临时表
    df.registerTempTable("log")

    //指标统计ispname
    val res = sqlContext.sql("select networkmannername as `网络类型`," +
      "sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) `总请求`," +
      "sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) `有效请求`," +
      "sum(case when requestmode = 1 and processnode =3 then 1 else 0 end) `广告请求`," +
      "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) `参与竞价数`," +
      "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) `竞价成功数`," +
      "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) `展示量`," +
      "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) `点击量`," +
      "sum(case when requestmode = 2 and iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end)/1000 `DSP广告消费`," +
      "sum(case when requestmode = 2 and iseffective = 1 and isbilling = 1 and iswin = 1 then 1 else 0 end)/1000 `DSP广告成本`" +
      "from log group by networkmannername").show()

    sc.stop()


  }

}
