package com.aikosolar.app.base.config

import java.util

import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class FLinkKafkaConfig extends FinkBaseConfig {

  @Option(names = Array("--bootstrap.servers"), required = true, description = Array("Kafka Bootstrap Servers"))
  var bootstrapServers: String = _

  @Option(names = Array("--group.id"), required = true, description = Array("Group Id"))
  var groupId: String = _

  @Option(names = Array("--topic"), required = true, description = Array("Kafka Topic"))
  var topic: String = _


  override def toMap: util.Map[String, String] = {

    val map: util.Map[String, String] = super.toMap

    map.put("--bootstrap.servers", this.bootstrapServers)
    map.put("--group.id", this.groupId)
    map.put("--topic", this.topic)

    map
  }
}
