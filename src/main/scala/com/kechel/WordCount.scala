package com.kechel

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //spark实现wordcount

    //设置输入输出路径
    val inpath = "D:\\test\\spark\\000.txt"
    val outpath = "D:\\test\\spark\\out"

    //构建sparkConf
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //wordcount
    sc.textFile(inpath).flatMap(_.split("\t")).map((_ , 1))
      .reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile(outpath)

    sc.stop()
  }

}
