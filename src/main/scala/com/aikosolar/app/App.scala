package com.aikosolar.app

import java.text.SimpleDateFormat
import java.util.Properties

import com.aikosolar.app.bean.halmfull
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, OutputTag}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import scala.collection.mutable


object App {
  def main(args: Array[String]): Unit = {
    // Flink流式环境的创建
    val env = FlinkUtils.initFlinkEnv()
    // 整合Kafka
    val consumer = FlinkUtils.initKafkaFlink()
    // 获取kafka信息
    val kafkaDataStream: DataStream[String] = env.addSource(consumer)
    //广播流kafka配置
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers", GlobalConfigUtil.bootstrapServers)
    props.setProperty("group.id", GlobalConfigUtil.groupId)
    props.setProperty("enable.auto.commit", GlobalConfigUtil.enableAutoCommit)
    props.setProperty("auto.commit.interval.ms", GlobalConfigUtil.autoCommitIntervalMs)
    props.setProperty("auto.offset.reset", GlobalConfigUtil.autoOffsetReset)


    //解析JSON
    val canalDs: DataStream[halmfull] = kafkaDataStream.map {
      json =>
        halmfull(json)
    }
    canalDs.print()
    val tableNameStryear: String = GlobalConfigUtil.tableNameStryear
    var columnName = GlobalConfigUtil.columnFamilyName
    canalDs.map(line=>{
      val key=line.rowkey
      val Hmap = Map(
        "site" -> line.site,
        "begin_date" -> line.begin_date,
        "shift"->line.shift,
        "uniqueid" -> line.uniqueid,
        "batchid" -> line.batchid,
        "title" -> line.title,
        "celltyp" -> line.celltyp,
        "begin_time" -> line.begin_time,
        "comments" -> line.comment,
        "operator" -> line.operator,
        "classification" -> line.classification,
        "bin" -> line.bin,
        "uoc" -> line.uoc,
        "isc" -> line.isc,
        "ff" -> line.ff,
        "eta" -> line.eta,
        "m3_uoc" -> line.m3_uoc,
        "m3_isc" -> line.m3_isc,
        "m3_ff" -> line.m3_ff,
        "m3_eta" -> line.m3_eta,
        "jsc" -> line.jsc,
        "iscuncorr" -> line.iscuncorr,
        "uocuncorr" -> line.uocuncorr,
        "impp" -> line.impp,
        "jmpp" -> line.jmpp,
        "umpp" -> line.umpp,
        "pmpp" -> line.pmpp,
        "ff_abs" -> line.ff_abs,
        "ivld1" -> line.ivld1,
        "jvld1" -> line.jvld1,
        "uvld1" -> line.uvld1,
        "pvld1" -> line.pvld1,
        "ivld2" -> line.ivld2,
        "jvld2" -> line.jvld2,
        "uvld2" -> line.uvld2,
        "pvld2" -> line.pvld2,
        "measuretimelf" -> line.measuretimelf,
        "crvuminlf" -> line.crvuminlf,
        "crvumaxlf" -> line.crvumaxlf,
        "monicellspectralmismatch" -> line.monicellspectralmismatch,
        "tmonilf" -> line.tmonilf,
        "rser" -> line.rser,
        "sser" -> line.sser,
        "rshunt" -> line.rshunt,
        "sshunt" -> line.sshunt,
        "tcell" -> line.tcell,
        "cellparamtki" -> line.cellparamtki,
        "cellparamtku" -> line.cellparamtku,
        "cellparamarea" -> line.cellparamarea,
        "cellparamtyp" -> line.cellparamtyp,
        "tenv" -> line.tenv,
        "tmonicell" -> line.tmonicell,
        "insol" -> line.insol,
        "insolmpp" -> line.insolmpp,
        "correctedtoinsol" -> line.correctedtoinsol,
        "cellidstr" -> line.cellidstr,
        "irevmax" -> line.irevmax,
        "rserlf" -> line.rserlf,
        "rshuntlf" -> line.rshuntlf,
        "monicellmvtoinsol" -> line.monicellmvtoinsol,
        "flashmonicellvoltage" -> line.flashmonicellvoltage,
        "el2class" -> line.el2class,
        "el2contactissue" -> line.el2contactissue,
        "el2crackdefaultcount" -> line.el2crackdefaultcount,
        "el2crystaldefaultarea" -> line.el2crystaldefaultarea,
        "el2crystaldefaultcount" -> line.el2crystaldefaultcount,
        "el2crystaldefaultseverity" -> line.el2crystaldefaultseverity,
        "el2darkdefaultarea" -> line.el2darkdefaultarea,
        "el2darkdefaultcount" -> line.el2darkdefaultcount,
        "el2darkdefaultseverity" -> line.el2darkdefaultseverity,
        "el2darkgrainboundary" -> line.el2darkgrainboundary,
        "el2edgebrick" -> line.el2edgebrick,
        "el2fingercontinuousdefaultarea" -> line.el2fingercontinuousdefaultarea,
        "el2fingercontinuousdefaultcount" -> line.el2fingercontinuousdefaultcount,
        "el2fingercontinuousdefaultseverity" -> line.el2fingercontinuousdefaultseverity,
        "el2fingerdefaultarea" -> line.el2fingerdefaultarea,
        "el2fingerdefaultcount" -> line.el2fingerdefaultcount,
        "el2fingerdefaultseverity" -> line.el2fingerdefaultseverity,
        "el2firing" -> line.el2firing,
        "el2grippermark" -> line.el2grippermark,
        "el2line" -> line.el2line,
        "el2oxring" -> line.el2oxring,
        "el2scratchdefaultarea" -> line.el2scratchdefaultarea,
        "el2scratchdefaultcount" -> line.el2scratchdefaultcount,
        "el2scratchdefaultseverity" -> line.el2scratchdefaultseverity,
        "el2spotdefaultarea" -> line.el2spotdefaultarea,
        "el2spotdefaultcount" -> line.el2spotdefaultcount,
        "el2spotdefaultseverity" -> line.el2spotdefaultseverity,
        "elbin" -> line.elbin,
        "elbincomment" -> line.elbincomment,
        "elcamexposuretime" -> line.elcamexposuretime,
        "elcamgain" -> line.elcamgain,
        "el2evalrecipe" -> line.el2evalrecipe,
        "pecon" -> line.pecon,
        "pectsys" -> line.pectsys,
        "pecyt" -> line.pecyt,
        "pepmt" -> line.pepmt,
        "pepreviouspinotreadyformeasurement" -> line.pepreviouspinotreadyformeasurement,
        "el2timeevaluation" -> line.el2timeevaluation,
        "m2_isc" -> line.m2_isc,
        "m2_uoc" -> line.m2_uoc,
        "m2_pmpp" -> line.m2_pmpp,
        "m2_ff" -> line.m2_ff,
        "m2_eta" -> line.m2_eta,
        "m2_insol" -> line.m2_insol,
        "m3_pmpp" -> line.m3_pmpp,
        "m3_insol" -> line.m3_insol,
        "m2_insol_m1" -> line.m2_insol_m1,
        "m2_insol_m2" -> line.m2_insol_m2,
        "insol_m1" -> line.insol_m1,
        "insol_m2" -> line.insol_m2,
        "m3_insol_m1" -> line.m3_insol_m1,
        "m3_insol_m2" -> line.m3_insol_m2,
        "eqp_id" -> line.eqp_id,
        "aoi_1_q" -> line.aoi_1_q,
        "aoi_1_r" -> line.aoi_1_r,
        "aoi_2_q" -> line.aoi_2_q,
        "aoi_2_r" -> line.aoi_2_r,
        "irev2" -> line.irev2,
        "irev1" -> line.irev1,
        "rshuntdr" -> line.rshuntdr,
        "rshuntdf" -> line.rshuntdf,
        "rserlfdf" -> line.rserlfdf,
        "bin_comment" -> line.bin_comment,
        "elmeangray" -> line.elmeangray,
        "rshuntdfdr"->line.rshuntdfdr
      )
      HBaseUtil.putMapData(tableNameStryear, key, columnName, Hmap)
    })
     // canalDs.process(new warnProcess)
    env.execute("halm")

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

  case class WarnData(msg:String,eqptype:String)*/
}

