package com.aikosolar.app.base.config

import picocli.CommandLine.Option

/**
  *
  * @author <a href="cheng.cao@zetatech.com.cn">carlc</a>
  */
class FinkBaseConfig {

  @Option(names = Array("--job-name"), required = false, description = Array("Fink jobName"))
  var jobName: String = _

  @Option(names = Array("--time-characteristic"), required = false,  description = Array("Fink jobName"))
  var timeCharacteristic: String = "ProcessingTime"

}