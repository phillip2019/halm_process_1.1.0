package com.aikosolar.app.tool

object test {
  def main(args: Array[String]): Unit = {
    val tool=new Tool_Str
    println("fin==========="+tool.Date_Shift_Time("Z1","2020-07-17 20:00:24"))

    val ss="1.24E-4"
    println(ss.toString)
  }
}
