package com.yxzc.tag

import java.text.SimpleDateFormat
import java.{io, util}

import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.yxzc.tag.utils.{MyEsUtils, PropertiesUtils, TagConstant}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import java.time.LocalDate
import java.util.Date

import scala.collection.mutable

object TestTag {
  def main(args: Array[String]): Unit = {
    var now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var time: String = dateFormat.format(now)
    if (args.length == 1) {
      val array: Array[String] = args.toArray
      time = array(0)
    } else {
      time = dateFormat.format(now)
    }

    println(time)


    System.setProperty("HADOOP_USER_NAME", "root")
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setAppName("SparkSQL01_Demo").setMaster("local[*]")

    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    spark.sql("show databases").show()

    spark.sql("use app")
    spark.sql("show tables").show()
    println("Done!")

    println("评分计算:")
    //val frame: DataFrame = spark.sql("select \n    collect_list(tagid) tagid,\n    ipid ip,\n    collect_list(tagweight) tagweight\nfrom app.profile_tag_ip\nwhere `dt`='2020-06-01'\ngroup by ipid")
    //frame.show()
    //spark.sql("with t1 as \n(\nselect \n    concat_ws(':',tagid,tagweight) a1,\n    ipid a2\nfrom app.profile_tag_ip\nwhere `dt`='2020-06-06'\ngroup by ipid,tagid,tagweight\n),t2 as \n(\nselect \n    collect_set(t1.a1) q1,\n    t1.a2 q2\nfrom t1 \ngroup by t1.a2\n)\nselect \n     split(sss,':')[0] as tag,\n     split(sss,':')[1] as tagweight,\n     t2.q2 ip\nfrom t2\nlateral view explode(q1) ss as sss").toDF().show()

    //    val frame: DataFrame = spark.sql("with t1 as \n(\nselect \n    concat_ws(':',tagid,tagweight) a1,\n    ipid a2\nfrom app.profile_tag_ip\nwhere `dt`>='2020-06-01' and `dt`<='2020-06-07'\ngroup by ipid,tagid,tagweight\n)\nselect \n    collect_set(t1.a1),\n    t1.a2\nfrom t1\ngroup by t1.a2").toDF()
    //    val frame: DataFrame = spark.sql("with t1 as \n(\nselect \n    concat_ws(':',tagid,tagweight) a1,\n    ipid a2\nfrom app.profile_tag_ip\nwhere `dt`='2020-06-02'\ngroup by ipid,tagid,tagweight\n)\nselect \n    collect_set(t1.a1),\n    t1.a2\nfrom t1\ngroup by t1.a2").toDF()


    //current_date   获取当前日期 2020-06-01
    var sql = "with t1 as (select     concat_ws(':',tagid,tagweight) a1,    ipid a2 from app.profile_tag_ip where `dt`='" + time + "' group by ipid,tagid,tagweight)select     collect_set(concat_ws(':',split(t1.a1,':')[0],split(t1.a1,':')[1])),    t1.a2 from t1 group by t1.a2"
        val frame: DataFrame = spark.sql("with t1 as \n(\nselect \n    concat_ws(':',tagid,tagweight) a1,\n    ipid a2\nfrom app.profile_tag_ip\nwhere `dt`='2020-06-07'\ngroup by ipid,tagid,tagweight\n)\nselect \n    collect_set(concat_ws(':',split(t1.a1,':')[0],split(t1.a1,':')[1])),\n    t1.a2\nfrom t1\ngroup by t1.a2").toDF()
//    val frame: DataFrame = spark.sql(sql).toDF()
    frame.show()

    val rows: util.List[Row] = frame.collectAsList()
    val value: Array[AnyRef] = rows.toArray()

    val n = value.length - 1
    for (i <- 0 to n) {
      // 获取es连接
      val jestClient = MyEsUtils.getClient()
      val map: util.HashMap[String, Any] = new util.HashMap[String, Any]()

      var num = 0L
      var weight = 0
      val str: String = value(i).toString.split("\\),")(0).split("\\(")(1)
      val ip: String = value(i).toString.split("\\),")(1).split("]")(0)
      //println(ip + "->" + str)
      var str2 = "{" + str + "}"
      map.put("tagList", str2)

      val iparr: Array[String] = ip.split("\\.")
      var ipboduan = iparr(0).toInt * 256 * 256 * 256 + iparr(1).toInt * 256 * 256 + iparr(2).toInt * 256 + iparr(3).toInt

      if (!(ipboduan > 167772160L && ipboduan < 184549375L || ipboduan > 2886729728L && ipboduan < 2887778303L || ipboduan > 3232235520L && ipboduan < 3232301055L)) {
        var strings: Array[String] = str.split(",")

        if (strings.length > 4) {
          //        println(strings.toList)
          var arrlength: Int = strings.length - 1
          for (i <- 0 to arrlength) {
            var tagid: String = strings(i).split(":")(0).trim
            tagid match {
              case "A11U001_001" => num = num.+(5L)
              case "A11U001_002" => num = num.+(5L)

              case "A11U005_001" => num = num.+(10L)
              case "A11U005_002" => num = num.+(9L)
              case "A11U005_003" => num = num.+(8L)

              case "A11U010_001" => num = num.+(10L)
              case "A11U010_002" => num = num.+(9L)
              case "A11U010_003" => num = num.+(8L)

              case "A11U011_001" => num = num.+(10L)
              case "A11U011_002" => num = num.+(9L)
              case "A11U011_003" => num = num.+(8L)

              case "A11U002_001" => num = num.+(10L)
              case "A11U002_002" => num = num.+(9L)
              case "A11U002_003" => num = num.+(8L)

              case "A11U003_001" => num = num.+(10L)
              case "A11U003_002" => num = num.+(9L)
              case "A11U003_003" => num = num.+(8L)

              case "A11U004_001" => num = num.+(1L)
              case "A11U004_002" => num = num.+(2L)
              case "A11U004_003" => num = num.+(4L)
              case "A11U004_004" => num = num.+(6L)
              case "A11U004_005" => num = num.+(8L)
              case "A11U004_006" => num = num.+(10L)
              case "A11U004_007" => num = num.+(12L)

              case "A11U006_001" => num = num.+(6L)
              case "A11U006_002" => num = num.+(8L)
              case "A11U006_003" => num = num.+(10L)

              case "A11U007_001" => num = num.+(10L)
              case "A11U007_002" => num = num.+(8L)
              case "A11U007_003" => num = num.+(6L)
              case "A11U007_004" => num = num.+(4L)
              case "A11U007_005" => num = num.+(2L)

              case "A11U008_001" => num = num.+(10L)
              case "A11U008_002" => num = num.+(8L)
              case "A11U008_003" => num = num.+(6L)
              case "A11U008_004" => num = num.+(4L)
              case "A11U008_005" => num = num.+(2L)

              case "A10U009_001" => num = num.+(10L)
              case "A10U009_002" => num = num.+(10L)
              case _ => num = num.+(0L)
            }
            //println(ip + "->" + num)
            map.put("ip", ip)
            map.put("sorce", num)
            map.put("tag_time", "2020-06-07")
            tagid match {
              case "A11U001_001" => weight = 5
              case "A11U001_002" => weight = 5

              case "A11U005_001" => weight = 10
              case "A11U005_002" => weight = 9
              case "A11U005_003" => weight = 8

              case "A11U010_001" => weight = 10
              case "A11U010_002" => weight = 9
              case "A11U010_003" => weight = 8

              case "A11U011_001" => weight = 10
              case "A11U011_002" => weight = 9
              case "A11U011_003" => weight = 8

              case "A11U002_001" => weight = 10
              case "A11U002_002" => weight = 9
              case "A11U002_003" => weight = 8

              case "A11U003_001" => weight = 10
              case "A11U003_002" => weight = 9
              case "A11U003_003" => weight = 8

              case "A11U004_001" => weight = 1
              case "A11U004_002" => weight = 2
              case "A11U004_003" => weight = 4
              case "A11U004_004" => weight = 6
              case "A11U004_005" => weight = 8
              case "A11U004_006" => weight = 10
              case "A11U004_007" => weight = 12

              case "A11U006_001" => weight = 6
              case "A11U006_002" => weight = 8
              case "A11U006_003" => weight = 10

              case "A11U007_001" => weight = 10
              case "A11U007_002" => weight = 8
              case "A11U007_003" => weight = 6
              case "A11U007_004" => weight = 4
              case "A11U007_005" => weight = 2

              case "A11U008_001" => weight = 10
              case "A11U008_002" => weight = 8
              case "A11U008_003" => weight = 6
              case "A11U008_004" => weight = 4
              case "A11U008_005" => weight = 2

              case "A10U009_001" => weight = 10
              case "A10U009_002" => weight = 10
              case _ => weight = 0
            }
            map.put(tagid, weight)
          }
          val str1: String = JSON.toJSONString(map, new SerializeConfig(true))
          println(str1)
          // JSON格式字符串
          //MyEsUtils.insertIntoEs(jestClient, "ip_tag", str1) //逐行插入
        }
      }
      jestClient.close()
    }
    spark.stop()
  }
}
