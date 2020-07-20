object test1 {
  def main(args: Array[String]): Unit = {
    val s:String="12:32:33"
    val t=s match {
      case s if s.contains("11")=>1
      case s if s.contains("32")=>2
      case _=>"other"
    }
    println(t)
  }
}
