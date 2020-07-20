package com.aikosolar.app

import java.util.Properties

import com.aikosolar.app.GlobalConfigUtil.config
import com.aikosolar.app.bean.{halmfull, oeedtl}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumerBase}



object OeeHalmdtl {
  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()
    // 整合Kafka
    var consumer:FlinkKafkaConsumerBase[String] = FlinkUtils.initKafkaFlink()
    if(args.length>0  && "E".equals(args(0))){
      consumer=consumer.setStartFromEarliest()
    } else if(args.length>0 && "L".equals(args(0))){
      consumer=consumer.setStartFromLatest()
    }else{
    }
      //.setStartFromEarliest()
    // 获取kafka信息
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    //广播流kafka配置
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    props.setProperty("group.id", GlobalConfigUtil.getConfig("oee.group.id"))
    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)


    //解析JSON
    val canalDs: DataStream[oeedtl] = kafkaDataStream.map {
      json =>
        oeedtl(json)
    }

    val hbaseTableName: String =GlobalConfigUtil.getConfig("oeedtltablename")
    val columnName =GlobalConfigUtil.getConfig("columnfamilyname")
    canalDs.map(line => {
      val key = line.rowkey
      val Hmap = Map(
        "batch_id" -> line.batchid,
        "test_time" -> line.testtime,
        "test_date" -> line.testdate,
        "site" -> line.site,
        "begin_time" -> line.begin_time,
        "begin_date" -> line.begin_date,
        "shift" -> line.shift,
        "eqp_id" -> line.eqp_id,
        "comments" -> line.comments,
        "bin" -> line.bin,
        "bin_comment" -> line.bin_comment,
        "uoc" -> line.uoc,
        "isc" -> line.isc,
        "ff" -> line.ff,
        "eta" -> line.eta,
        "m3_eta" -> line.m3_eta,
        "irev2" -> line.irev2,
        "rser" -> line.rser,
        "rshunt" -> line.rshunt,
        "tcell" -> line.tcell,
        "tmonicell" -> line.tmonicell,
        "elmeangray" -> line.elmeangray,
        "aoi_2_r" -> line.aoi_2_r,
        "el2fingerdefaultcount" -> line.el2fingerdefaultcount,
        "cellparamtki" -> line.cellparamtki,
        "cellparamtku" -> line.cellparamtku,
        "insol_m1" -> line.insol_m1,
        "m3_insol" -> line.m3_insol,
        "bin_type"->line.bin_type,
        "order_type"->line.order_type
      )
      HBaseUtil.putMapData(hbaseTableName, key, columnName, Hmap)
    })
    // canalDs.process(new warnProcess)
    env.execute("OeeHalmdtl")

  }
}



  /*class warnProcess extends ProcessFunction[halmfull,WarnData]{
    //定义一个用于记录定时器时间的状态
    lazy val currentTimer = getRuntimeContext.getState[Long](new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))

    override def processElement(i: halmfull, context: ProcessFunction[halmfull, WarnData]#Context, collector: Collector[WarnData]): Unit = {
      val currentTemp=currentTimer.value()
      val time=currentTemp+10*60*1000
      val current = context.timerService().currentProcessingTime()
      if(current>time || currentTemp==0 ){
        //一个定时器
        context.timerService().registerProcessingTimeTimer(time)
        currentTimer.update(time)
      }else{
        context.timerService().deleteProcessingTimeTimer(currentTemp)
        currentTimer.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: ProcessFunction[halmfull, WarnData]#OnTimerContext, out: Collector[WarnData]): Unit = {
        out.collect(WarnData("halm","warn"))
      currentTimer.clear()
    }
  }

  case class WarnData(msg:String,eqptype:String)
}

*/
