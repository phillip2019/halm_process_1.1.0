package com.aikosolar.app.base

import java.lang.reflect.{ParameterizedType, Type}

import com.aikosolar.app.base.config.FinkBaseConfig
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import picocli.CommandLine

/**
  *
  * @author carlc
  */
abstract class FlinkRunner[C <: FinkBaseConfig] {

  final def main(args: Array[String]): Unit = {
    runWith(args)
  }


  def runWith(args: Array[String]): Unit = {
    for (arg <- args) println(arg)

    val config: C = parseConfig(args)

    this.validate(config)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 仅帮助设置下而,当为eventTime时自行指定
    env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(config.timeCharacteristic))

    this.setupEnv(env, config)

    env.getConfig.setGlobalJobParameters(config)

    this.run(env, config)

    val jName = jobName(config)

    if (jName == null || "".equals(jName.trim)) env.execute() else env.execute(jName)
  }

  def parseConfig(args: Array[String]): C = {
    // 个人不喜欢FLink提供的解析参数工具,所以用第三方的
    val pType: ParameterizedType = (this.getClass.getGenericSuperclass).asInstanceOf[ParameterizedType]
    val actualTypeArguments: Array[Type] = pType.getActualTypeArguments
    val t: Type = actualTypeArguments(0)
    val c: Class[C] = t.asInstanceOf[Class[C]]
    val instance: C = c.newInstance()
    new CommandLine(instance).parse(args: _*)
    instance
  }

  /**
    * 获取任务名称,默认为配置项中jobName
    */
  def jobName(c: C): String = c.jobName

  /**
    * 配置校验
    */

  def validate(c: C): Unit = {
    if ("prod".equalsIgnoreCase(c.runMode)){
      if(StringUtils.isBlank(c.checkpointDataUri)){
        throw new IllegalArgumentException("-- checkpointDataUri is required")
      }
    }
  }

  /**
    * 构建/初始化 env
    */
  def setupEnv(env: StreamExecutionEnvironment, c: C): Unit = {}

  /**
    * 业务方法[不需自己调用env.execute()]
    */
  def run(env: StreamExecutionEnvironment, c: C)

//  def createSource[T: TypeInformation](c: C): SourceFunction[T]
}
