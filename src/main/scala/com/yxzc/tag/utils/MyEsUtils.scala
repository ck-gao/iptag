package com.yxzc.tag.utils

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, DocumentResult, Index}


object MyEsUtils {
  val esUrl: String ="http://ats1:9200"  // es 集群地址
  val factory: JestClientFactory = new JestClientFactory()  // 创建工厂类
  val config: HttpClientConfig = new HttpClientConfig.Builder(esUrl) // 设置配置参数
    .multiThreaded(true)
    .maxTotalConnection(20)
    .connTimeout(10000)
    .readTimeout(10000)
    .build()
  factory.setHttpClientConfig(config) //给工厂类设置参数

  // 定义方法返回客户端
  def getClient(): JestClient ={
    factory.getObject
  }

  // 逐条写入
  def insertIntoEs(jestClient: JestClient,indexName:String,source:Any)={
    val indexAction: Index = new Index.Builder(source).index(indexName).`type`("repo").build()
    jestClient.execute(indexAction)
    println("*********************")
  }

  // 逐条写入
  def insertIntoEs(indexName:String,source:Any)={
    val client: JestClient = factory.getObject
    val indexAction: Index = new Index.Builder(source).index(indexName).`type`("repo").build()
    client.execute(indexAction);
    client.close()
  }

  /**
   * 批量插入 对接Rdd的分区数据
   * @param indexName
   * @param sources
   */
  def insertBulk(indexName:String,sources: Iterator[Any])={
    val client: JestClient = factory.getObject
    val bulkBilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    // 对数据进行匹配
    sources.foreach(
      {
        case (id:String,source)=>{ // 如果传递了Id则使用传的Id
          bulkBilder.addAction(new Index.Builder(source).id(id).build())
        }
        case source=>{
          bulkBilder.addAction(new Index.Builder(source).build()) //没传Id则使用随机生成的
        }
      }
    )
    client.execute(bulkBilder.build())
    client.close()
  }
}

