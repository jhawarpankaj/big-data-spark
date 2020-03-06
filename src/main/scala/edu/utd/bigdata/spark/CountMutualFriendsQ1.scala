package edu.utd.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object CountMutualFriendsQ1 {

  def main(args : Array[String]): Unit = {

    if (args.length != 2) {
      println("Invalid input parameters. Required: <input_file> <output_path>")
      return
    }
    // Initializing job configs...
    val conf = new SparkConf().setAppName("CountMutualFriends").setMaster("local")
    val sc = new SparkContext(conf)

    // Reading input arguments...
    val inputFile = args(0)
    val outputFile = args(1)

    // Read the input file and create an RDD...
    val dataFile = sc.textFile(inputFile)

    // For each line, form a pair with all the
    // friends and attach the entire list of friends...
    val pairWiseAllFriends = dataFile.flatMap(line => {
      val split = line.split("\\t")
      val myId = split(0)
      val allFriends = if (split.length > 1) split(1) else "Null"
      val friendsList = allFriends.split(",")
      //      val friendsListInt = for (i <- 0 to friendsListString.length - 1) yield friendsListString(i).toInt
      val friendTuple = friendsList.map(friendId => {
        if (myId < friendId) myId + "," + friendId else friendId + "," + myId
      })
      // attach the entire list of friends with every pair.
      friendTuple.map(aPair => (aPair, friendsList.toSet))
    })
    //    pairWiseAllFriends.collect().foreach(println)

    val mutualFriends = pairWiseAllFriends
      .reduceByKey(_ intersect _)
      .filter(eachKeyVal => !eachKeyVal._2.equals("Null"))
      .filter(eachKeyVal => !eachKeyVal._2.isEmpty)
      .sortByKey()

    val result = mutualFriends.map(eachKeyVal => eachKeyVal._1 + "\t" + eachKeyVal._2.size)
//    result.collect().foreach(println)
    result.saveAsTextFile(outputFile)
    //    mutualFriends.collect().foreach(println)
  }
}
