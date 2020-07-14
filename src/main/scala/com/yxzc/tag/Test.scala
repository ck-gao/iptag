package com.yxzc.tag

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object Test {
  def main(args: Array[String]): Unit = {
    //    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    //    val sc: SparkContext = new SparkContext(conf)
    //    val sqlContext = new SQLContext(sc)
    val conf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)

    System.setProperty("HADOOP_USER_NAME", "root")


    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val frame: DataFrame = spark.sql("select ipid,tagid from app.profile_tag_ip where date_format(dt,'yyyy-MM-dd')='2020-06-01'")


    val rows: util.List[Row] = frame.collectAsList()
    val value: Array[AnyRef] = rows.toArray()
    val length: Int = value.length
    var buffer: ListBuffer[(String, String)] = new ListBuffer
    for (i <- 0 to length - 1) {
      val strings: Array[String] = value(i).toString.split(",")
      println(strings(0).split("\\[")(1), strings(1).split("\\]")(0))
      buffer.append((strings(0).split("\\[")(1), strings(1).split("\\]")(0)))
    }
    var data1: Array[(String, String)] = buffer.toArray
    val data: RDD[(String, String)] = sc.makeRDD(data1)

    // 测试数据， 为方便分析问题
    // 左边一列是用户，有三个用户a,b,c
    // 右边一列是公司，表示用户喜欢的公司
    val testData = Array(
      ("a", "google"),
      ("a", "apple"),
      ("a", "mi"),
      ("b", "google"),
      ("b", "apple"),
      ("c", "google")
    )
    //val data = sc.parallelize(data1)

    //val data: RDD[(String, String)] = sc.makeRDD()

    //支持度：同时出现的概论
    //置信度: 购买x的人，同时购买Y的概率

    // 最终我们要构造出这样的结果：公司A、公司B、支持度、A->B的置信度、B->A的置信度
    // 要求支持度和置信度就需要三个值，喜欢A公司的人数，喜欢B公司的人数，同时喜欢A和B公司的人数
    // 我们先求前两个
    val companyCountRDD = data.map(a => (a._2, 1)).reduceByKey(_ + _)

    /**
     * (mi,1)
     * (google,3)
     * (apple,2)
     */
    companyCountRDD.collect().foreach(println)

    // 要计算同时喜欢A和B公司的人数，要先知道A，B所有可能的组合
    // 比如：1， 2， 3,；所有可能的组合就是（1,2）,（1,3）,（2,3）
    // 这里我们简单的用cartesian算子实现
    // cartesian算子会得到这样的结果：
    // （1,1），（1,2），（1,3），
    // （2,1），（2,2），（2,3），
    // （3,1），（3,2），（3,3）
    // 然后filter算子，只保留左边大于右边的结果，这样能过滤掉相等的结果，如（1,1），还有重复的结果，如（2,1），因为我们已经有（1,2）了
    val cartesianRDD = companyCountRDD.cartesian(companyCountRDD).filter(tuple => tuple._1._1 > tuple._2._1).map(t => ((t._1._1, t._2._1), (t._1._2, t._2._2)))

    // 这样我们不但得到了A和B的所有组合，还顺带聚合了计算用的到的数据
    /** 公司A、公司B、喜欢A公司的人数、喜欢B公司的人数
     * ((mi,google),(1,3))
     * ((mi,apple),(1,2))
     * ((google,apple),(3,2))
     */
    cartesianRDD.collect().foreach(println)

    // 下面开始计算，同时喜欢A和B公司的人数
    // 比如a这个人，它喜欢google,apple,mi; 那么就是同时喜欢(mi,google)，(mi,apple)，(google,apple)
    // 所以我们先要将数据转换成(a, (google,apple,mi))
    // 这个时候用户就没用了，我们只需要知道公司的组合
    // 因此转换成(mi,google)，(mi,apple)，(google,apple)
    // 最后用flatMap将结果打散，再计数
    val userCompaniesRDD = data.groupByKey().cache()
    val meanwhileRDD = userCompaniesRDD.map(_._2)
      // 这里采用了类似cartesian的做法计算所有的组合，然后过滤掉不需要的
      .flatMap(iter => iter.flatMap(i => iter.map(j => (i, j))).filter(tuple => tuple._1 > tuple._2))
      .map(tuple => (tuple, 1))
      .reduceByKey(_ + _)
    // 计算用户总数，后面会用到
    val userNum = userCompaniesRDD.count()

    /** 公司A、公司B、同时喜欢A和B公司的人数
     * ((mi,apple),1)
     * ((mi,google),1)
     * ((google,apple),2)
     */
    meanwhileRDD.collect().foreach(println)

    val calRDD = cartesianRDD.join(meanwhileRDD)

    /** 公司A、公司B、喜欢A公司的人数，喜欢B公司的人数，同时喜欢A和B公司的人数
     * ((mi,apple),((1,2),1))
     * ((mi,google),((1,3),1))
     * ((google,apple),((3,2),2))
     */
    calRDD.collect.foreach(println)

    // 计算结果
    val resultRDD = calRDD.map(t => {
      val aCompany = t._1._1
      val bCompany = t._1._2
      val aCount = t._2._1._1
      val bCount = t._2._1._2
      val aAndbCount = t._2._2 * 1.0
      // 公司A、公司B、支持度、A->B的置信度、B->A的置信度
      (aCompany, bCompany, aAndbCount / userNum, aAndbCount / aCount, aAndbCount / bCount)
    })

    /**
     * (mi,apple,0.3333333333333333,1.0,0.5)
     * (mi,google,0.3333333333333333,1.0,0.3333333333333333)
     * (google,apple,0.6666666666666666,0.6666666666666666,1.0)
     */
    resultRDD.collect.foreach(println)

    // 最后可以过滤掉数值太低的
    // 支持度的阈值是1%，置信度阈值50%
    val support = 0.01
    val confidence = 0.5
    resultRDD.filter(a => a._3 > support && a._4 > confidence && a._5 > confidence).collect().foreach(println)

  }
}


