package com.aikosolar.app.bean

import java.text.SimpleDateFormat

import com.aikosolar.app.OeeHalmdtl
import com.aikosolar.app.tool.Tool_Str
import com.alibaba.fastjson.JSON


case class oeedtl(
                   var rowkey :String,
                   var batchid:String,
                   var testtime:String,
                   var testdate:String,
                   var site:String,
                   var begin_time:String,
                   var begin_date:String,
                   var shift:String,
                   var eqp_id:String,
                   var comments:String,
                   var bin:String,
                   var bin_comment:String,
                   var uoc:String,
                   var isc:String,
                   var ff:String,
                   var eta:String,
                   var m3_eta:String,
                   var irev2:String,
                   var rser:String,
                   var rshunt:String,
                   var tcell:String,
                   var tmonicell:String,
                   var elmeangray:String,
                   var aoi_2_r:String,
                   var el2fingerdefaultcount:String,
                   var cellparamtki:String,
                   var cellparamtku:String,
                   var insol_m1:String,
                   var m3_insol:String,
                   var order_type:String,
                   var bin_type:String
)
object oeedtl {
  def apply(json: String): oeedtl = {
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
    //分区字段
    val sdf1 = new SimpleDateFormat("dd-MM-yyyy")
    val tool_str=new Tool_Str()
    val js = JSON.parseObject(json)
    val testtime=sdf2.format(sdf1.parse(tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestDate", "")))).toString + " " + tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestTime", ""))
    val eqp_id=tool_str.isEmpty(js.getJSONObject("data").getOrDefault("eqp_id", ""))
    val time=tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestTime", ""))
    val time1=if(time==null || time.length<5) "" else time.substring(0,5)
    val hbrowkey= time1+"|"+eqp_id+"|"+testtime
    val site=if(eqp_id==null || eqp_id.length<2) "" else eqp_id.substring(0,2)
    val Begin_Date=if(testtime==null || testtime.length<10) "" else testtime.substring(0,10)
    val shift=tool_str.Date_Shift_Time(site,testtime)
    val comments=tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Comment", "")).toUpperCase()
    val bin=tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN", ""))
    val bin_type=if("0".equals(bin) || "100".equals(bin)) bin else "OTHER"
    val order_type=comments match {
      case comments if comments.contains("YP")=>"YP"
      case comments if comments.contains("PL")=>"PL"
      case comments if comments.contains("BY")=>"BY"
      case comments if comments.contains("YZ")=>"YZ"
      case comments if comments.contains("LY")=>"LY"
      case comments if comments.contains("CFX")=>"CFX"
      case comments if comments.contains("CC")=>"CC"
      case comments if comments.contains("GSQ")=>"GSQ"
      case comments if comments.contains("GSH")=>"GSH"
      case comments if comments.contains("CID")=>"CID"
      case _=>"NORMAL"
    }
    val oeehalmdtl = oeedtl(
      hbrowkey,
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BatchID", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestTime", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("TestDate", "") ),
      site,
      testtime,
      Begin_Date,
      shift,
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("eqp_id", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Comment", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("BIN_Comment", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Uoc", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Isc", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("FF", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Eta", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Eta", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("IRev2", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Rser", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Rshunt", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Tcell", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Tmonicell", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("ELMeanGray", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("AOI_2_R", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("EL2FingerDefaultCount", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamTkI", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("CellParamTkU", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("Insol_M1", "") ),
      tool_str.isEmpty(js.getJSONObject("data").getOrDefault("M3_Insol", "") ) ,
      bin_type,
      order_type
    )
    oeehalmdtl
  }
}





