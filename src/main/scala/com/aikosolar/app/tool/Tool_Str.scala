package com.aikosolar.app.tool

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSONObject

 class Tool_Str {
  /**
   * 判断字符串是否为空
   * @param obj 字符串
   * @return 是否为空
   */
  def isEmpty(obj: Object):String = {
    val s=if(obj == null) "" else obj.toString
     s
  }


  def Date_Shift_Time(site:String,str:String): String ={
   var starthour,endhour,min= 0
   val DateStrdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
   val cal = Calendar.getInstance
   var data: Date = null
   var strdata = ""
   data = DateStrdf.parse(str)
   val timesub=str.substring(11,16)
   cal.setTime(data)
   if(site.startsWith("G")){
    starthour=7
    endhour=19
    min=30
    if(cal.get(Calendar.HOUR_OF_DAY)>=starthour && cal.get(Calendar.MINUTE)>=min && cal.get(Calendar.HOUR_OF_DAY)<endhour ){
     cal.add(Calendar.DAY_OF_YEAR, 0)
     strdata = DateStrdf.format(cal.getTime).substring(0, 10) + "-D"
    }else if(cal.get(Calendar.HOUR_OF_DAY)>starthour && cal.get(Calendar.MINUTE)<min && cal.get(Calendar.HOUR_OF_DAY)<=endhour){
     cal.add(Calendar.DAY_OF_YEAR, 0)
     strdata = DateStrdf.format(cal.getTime).substring(0, 10) + "-D"
    }
    else {
     if(cal.get(Calendar.HOUR_OF_DAY)>=0 && cal.get(Calendar.HOUR_OF_DAY)<=starthour && cal.get(Calendar.MINUTE)<min ){
      cal.add(Calendar.DAY_OF_YEAR, -1)
     }
     strdata = DateStrdf.format(cal.getTime).substring(0, 10) + "-N"
    }
   }else{
    starthour=8
    endhour=20
    if(cal.get(Calendar.HOUR_OF_DAY)>=starthour && cal.get(Calendar.HOUR_OF_DAY)<endhour ){
     cal.add(Calendar.DAY_OF_YEAR, 0)
     strdata = DateStrdf.format(cal.getTime).substring(0, 10) + "-D"
    }else {
     if(cal.get(Calendar.HOUR_OF_DAY)>=0 && cal.get(Calendar.HOUR_OF_DAY)<starthour){
      cal.add(Calendar.DAY_OF_YEAR, -1)
     }
     strdata = DateStrdf.format(cal.getTime).substring(0, 10) + "-N"
    }
   }
    strdata
  }


  def getDayDate(site:String,time:String): String ={
   val DateStrdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
   val cal = Calendar.getInstance
   val data = DateStrdf.parse(time)
   var strdata=""
   cal.setTime(data)
   if(site.startsWith("G")){
    cal.add(Calendar.HOUR_OF_DAY, -7)
    cal.add(Calendar.MINUTE,-30)
   }else{
    cal.add(Calendar.HOUR_OF_DAY, -8)
   }
   DateStrdf.format(cal.getTime).substring(0, 10)
  }


}
