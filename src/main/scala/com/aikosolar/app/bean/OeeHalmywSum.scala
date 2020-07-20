package com.aikosolar.app.bean

import java.text.SimpleDateFormat

import com.aikosolar.app.tool.Tool_Str
import com.alibaba.fastjson.JSON


case class OeeHalmywSum(
    var rowkey: String,
    var DAYDATE:String,
    var DAYHOUR:String,
    var TWOHOUR:String,
    var HALFHOUR:String,
    var CREATE_TIME:String,
    var EQP_ID:String,
    var shift:String,
    var site:String,
    var COMMENTS:String,
    var ORDERTYPE:String,
    var bin_type:String,
    var OUTPUT:String,
    var OUTPUT2:String,
    var UOC:String,
    var ISC:String,
    var FF:String,
    var ETA:String,
    var M3_ETA:String,
    var IREV2:String,
    var RSER:String,
    var RSHUNT:String,
    var TCELL:String,
    var TMONICELL:String,
    var INSOL_M1:String,
    var M3_INSOL:String,
    var NUM_A:String,
    var NUM_ZHENGMIANYICHANG:String,
    var NUM_BEIMIANYICHANG:String,
    var NUM_YANSEYICHANG:String,
    var NUM_YANSEYICHANG2:String,
    var NUM_MO5:String,
    var NUM_IREV2:String,
    var NUM_RSH:String,
    var NUM_DX:String,
    var NUM_DUANSHAN:String,
    var NUM_HUASHANG:String,
    var NUM_HEIDIAN:String,
    var NUM_KEXINDU:String,
    var NUM_YINLIE:String,
    var NUM_COLOR_ALL:String,
    var NUM_COLOR_A:String,
    var NUM_COLOR_B:String,
    var NUM_COLOR_C:String,
    var NUM_DS_ALL:String,
    var NUM_DS_0:String,
    var NUM_DS_1:String,
    var NUM_DS_2:String,
    var NUM_DS_3:String,
    var NUM_DS_4:String,
    var NUM_DS_5:String,
    var NUM_DS_5P:String,
    var NUM213:String,
    var NUM214:String,
    var NUM215:String,
    var NUM216:String,
    var NUM217:String,
    var NUM218:String,
    var NUM219:String,
    var NUM220:String,
    var NUM221:String,
    var NUM222:String,
    var NUM223:String,
    var NUM224:String,
    var NUM225:String,
    var NUM226:String,
    var NUM227:String,
    var NUM228:String,
    var NUM229:String,
    var NUM230:String,
    var NUM231:String,
    var NUM232:String,
    var NUM233:String,
    var NUM234:String,
    var NUM235:String,
    var NUM236:String,
    var NUM237:String,
    var NUM238:String,
    var NUM239:String,
    var NUM240:String
)

object OeeHalmywSum {
  def apply(json: String): OeeHalmywSum = {
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
   /* val oeehalmywsum = OeeHalmywSum(
      hbrowkey,

    )*/

    null
  }
}





