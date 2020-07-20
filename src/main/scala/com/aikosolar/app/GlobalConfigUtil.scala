package com.aikosolar.app


import com.typesafe.config.{Config, ConfigFactory}
// 配置文件加载类
object GlobalConfigUtil {

  // 通过工厂加载配置
  val config:Config = ConfigFactory.load()
  //FlinkUtils配置加载

  val checkpointDataUri:String=config.getString("checkpoint.Data.Uri")
  val bootstrapServers:String = config.getString("bootstrap.servers")
  val groupId:String = config.getString("group.id")
  val enableAutoCommit:String = config.getString("enable.auto.commit")
  val autoCommitIntervalMs: String = config.getString("auto.commit.interval.ms")
  val autoOffsetReset:String = config.getString("auto.offset.reset")
  val zookeeperConnect:String = config.getString("zookeeper.connect")
  val inputTopic:String = config.getString("input.topic")
  //HBase的配置加载

  val tableNameStryear: String = config.getString("tablenamestryear")
  val columnFamilyName:String = config.getString("columnfamilyname")
  def getConfig(key:String): String ={
    config.getString(key)
  }




  def main(args: Array[String]): Unit = {
    println(bootstrapServers)
    println(zookeeperConnect)
    println(inputTopic)
    println(groupId)
    println(enableAutoCommit)
    println(autoCommitIntervalMs)
    println(autoOffsetReset)
  }

}
