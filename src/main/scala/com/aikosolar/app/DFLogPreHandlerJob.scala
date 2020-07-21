package com.aikosolar.app

import java.util.Properties

import com.aikosolar.app.base.FlinkRunner
import com.aikosolar.app.conf.DFLogPreHandleConfig
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

/**
  *
  * DF日志数据预处理器
  *
  * input: kafka
  *
  * output: kafka
  *
  * 逻辑: 根据特定的开始、结束标志截取开始-结束之前的数据，合并后发送到目标kafka topic,不做其他业务逻辑处理(由下游进行逻辑加工)
  *
  * 运行参数:
  *
  * --job-name=df-log-job
  * --bootstrap.servers=127.0.0.1:9092
  * --topic=t1
  * --group.id=g1
  * --target.bootstrap.servers=127.0.0.1:9092
  * --target.topic=t2
  *
  * @author carlc
  */
object DFLogPreHandlerJob extends FlinkRunner[DFLogPreHandleConfig] {
  override def run(env: StreamExecutionEnvironment, c: DFLogPreHandleConfig): Unit = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", c.bootstrapServers)
    props.setProperty("group.id", c.groupId)

    val source = new FlinkKafkaConsumer010[String](c.topic, new SimpleStringSchema, props)

    val stream = env.addSource(source)
      .filter(StringUtils.isNotBlank(_))
      .assignAscendingTimestamps(x => DateUtils.parseDate(x.substring(0, 19), "MM/dd/yyyy HH:mm:ss").getTime)
//      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String] {
//        override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = new Watermark(extractedTimestamp)
//
//        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = try
//          return DateUtils.parseDate(element.substring(0, 19), "MM/dd/yyyy HH:mm:ss").getTime
//        catch {
//          case e:Exception =>
//            return previousElementTimestamp
//        }
//      })
      .map(("dummyKey", _)) // 由于后续需要keyedState,所以这里添加一个无逻辑含义的字段
      .keyBy(_._1)
      .process(new MergeFunction())

    stream.print()
  }


  /**
    * 构建/初始化 env
    */
  override def setupEnv(env: StreamExecutionEnvironment, c: DFLogPreHandleConfig): Unit = {
    env.setParallelism(c.parallelism)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  }

  override def jobName(c: DFLogPreHandleConfig): String = c.jobName

  class MergeFunction extends KeyedProcessFunction[String, (String, String), String] {
    lazy val recordsState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[String, Seq[String], String]("records-state", new DFLogAggregateFunction, implicitly[TypeInformation[Seq[String]]]))
    lazy val beginState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("begin-state", classOf[Boolean]))
    lazy val endState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("end-state", classOf[Boolean]))
    lazy val timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    lazy val missBeginStream = new OutputTag[String]("miss-begin-stream")
    lazy val missEndStream = new OutputTag[String]("miss-end-stream")

    override def processElement(value: (String, String), ctx: KeyedProcessFunction[String, (String, String), String]#Context, out: Collector[String]): Unit = {
      val line = value._2
      val trimLine = line.trim
      val lowerCaseLine = trimLine.toLowerCase

      if (lowerCaseLine.contains("recipe start recipe:")) {
        recordsState.add(trimLine)
        beginState.update(true)
        if (!endState.value()) {
          if (timerState.value == 0L) {
            val ts = ctx.timerService.currentWatermark + 2 * 60 * 1000
            ctx.timerService.registerEventTimeTimer(ts)
            timerState.update(ts)
          }
          return
        }
        out.collect(recordsState.get)

        recordsState.clear()
        beginState.clear()
        endState.clear()
        timerState.clear()
      } else if (lowerCaseLine.contains("recipe end recipe:")) {
        recordsState.add(trimLine)
        endState.update(true)
        if (!beginState.value()) {
          if (timerState.value == 0L) {
            val ts = ctx.timerService.currentWatermark + 2 * 60 * 1000
            ctx.timerService.registerEventTimeTimer(ts)
            timerState.update(ts)
          }
          return
        }
        out.collect(recordsState.get)

        recordsState.clear()
        beginState.clear()
        endState.clear()
        timerState.clear()
      } else if (beginState.value || endState.value) {
        recordsState.add(trimLine)
      } else {
        // todo: wtf? 正常来讲不可能begin & end标志都为false，但不排除这种奇葩数据
        // todo: 记录日志或者输出到侧输出流
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, String), String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)

      ctx.output(if (beginState.value()) missEndStream else missBeginStream,recordsState.get())

      recordsState.clear()
      beginState.clear()
      endState.clear()

      timerState.clear()
    }
  }

  class DFLogAggregateFunction extends AggregateFunction[String, Seq[String], String] {

    override def createAccumulator(): Seq[String] = Seq[String]()

    override def add(in: String, acc: Seq[String]): Seq[String] = acc :+ in

    override def getResult(acc: Seq[String]): String = acc.mkString(" ")

    override def merge(acc1: Seq[String], acc2: Seq[String]): Seq[String] = acc1 ++ acc2
  }

}
