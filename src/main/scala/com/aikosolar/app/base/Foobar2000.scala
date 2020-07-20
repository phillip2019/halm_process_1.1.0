package com.aikosolar.app.base

import java.util.Properties

import com.aikosolar.app.base.config.FinkBaseConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

/**
  * 仅测试FlinkRunner是否工作正常
  *
  * @author carlc
  */
object Foobar2000 extends FlinkRunner[FinkBaseConfig] {
  override def run(env: StreamExecutionEnvironment, c: FinkBaseConfig): Unit = {
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", "127.0.0.1:9092")
    props.setProperty("group.id", "G1")

    env.addSource(new FlinkKafkaConsumer010[String]("t1", new SimpleStringSchema, props))
      .print()
  }
  override def jobName(c: FinkBaseConfig): String = "foobar2000"
}
