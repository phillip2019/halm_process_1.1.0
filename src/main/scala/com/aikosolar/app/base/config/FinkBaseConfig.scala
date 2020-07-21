package com.aikosolar.app.base.config

import java.util

import org.apache.flink.api.common.ExecutionConfig
import picocli.CommandLine.Option

/**
  *
  * @author carlc
  */
class FinkBaseConfig extends ExecutionConfig.GlobalJobParameters {

  @Option(names = Array("--job-name"), required = true)
  var jobName: String = _

  @Option(names = Array("--time-characteristic"), required = false)
  var timeCharacteristic: String = "ProcessingTime"

  @Option(names = Array("--parallelism"), required = false)
  var parallelism: Int = Runtime.getRuntime.availableProcessors()

  override def toMap: util.Map[String, String] = {
    val map: util.Map[String, String] = new util.HashMap[String, String]

    map.put("--job-name", this.jobName)
    map.put("--time-characteristic", this.timeCharacteristic)
    map.put("--parallelism", this.parallelism.toString)

    map
  }
}