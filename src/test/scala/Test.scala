object Test {
  def main(args: Array[String]): Unit = {
//    val s1 = "1,2,3,4"
//    val s2 = "1,2,3,4,5"
//    val a = s1.split(",")
//    val b = s2.split(",")
//    println(a.toSet intersect b.toSet)

    val temp = "null"
    val nfriends = temp.split(",")
    println(nfriends.length)
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    for(i <- 0 to friends.length - 1) println(friends(i))
//    println(friends(0).equals(0))


  }
}
