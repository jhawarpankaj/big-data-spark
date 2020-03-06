package edu.utd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top10 {

  def main(args: Array[String]) : Unit = {

    if(args.length != 3) {
      println("Invalid input params. Required: <input_connections_file> " +
        "<user_data_file> <output_path>")
      return
    }

    // Initializing job configs...
    val conf = new SparkConf().setAppName("Top10").setMaster("local")
    val sc = new SparkContext(conf)

    // Reading input arguments...
    val connFile = args(0)
    val userDataFile = args(1)
    val outputPath = args(2)

    // Input connections File as RDD...
    val conn : RDD[String] = sc.textFile(connFile)

    // Get mutual friends for every pair...
    val mFriends: RDD[(String, Set[String])] = conn.flatMap(getMutual)

      // All similar keys are group together.
      // hadoop like shuffle-sort phase just before reduce
      // this will o/p the list of mutual friends of every pair.
      .reduceByKey(_ intersect _)

      // _ denotes every element. In a PAIR RDD:
      // _._1 can be used to access the key
      // _._2 can be used to access the value
      // we filter the value where no friends. head selects the first element of the iterable.
      .filter(!_._2.isEmpty).filter(!_._2.head.equals("null"))

    // Getting count of mutual friends.
    // e.g., sample o/p will be (18681,18707 99)
    val mutualFriendsCount: RDD[(String, Int)] = mFriends.map(row => (row._1, row._2.size))
        .map(row => row.swap)
        .sortByKey(false, 1)
        .map(row => row.swap)

    // Extracting the top 10 from the entire list.
    val top10: RDD[(String, Int)] = sc.parallelize(mutualFriendsCount.take(10))

    // Splitting the data per user.
    // sample o/p will be: (The keys are kept to do a join and values to perform reduce later)
    // 18681 18681,18707:99
    // 18707 18681,18707:99
    val top: RDD[(String, String)] = top10.flatMap(row => {
      val users = row._1.split(",")
      val totalFriends = row._2.toString
      users.map(user => (user, row._1 + ":" + totalFriends.toString))
    })

    // Input user data file
    val data: RDD[String] = sc.textFile(userDataFile)
    case class User(fName: String, lName: String, address: String)

    // Reading the data file and extracting required information.
    val userDetails: RDD[(String, User)] = data.flatMap(line => {
      val details = line.split(",")
      val data = Array(for (i <- 0 until details.length - 1) yield details(i))
      data.map(info => (info(0), User(info(1), info(2), info(3))))
    })

    // Join...
    // produces a nested pair RDD.
    val joinedRecords: RDD[(String, (String, User))] = top.join(userDetails)
//    joinedRecords.foreach(println)

    // joinedRecords sample: (18707,(18681,18707:99,User(Raymond,Norman,2937 Black Stallion Road)))
    // The result is a pair RDD, hence we can then perform a reduceByKey() or a groupByKey() later
    // to merge records.
    // Making the value "18681,18707:99" as the key of the RDD so that we can merge
    // the records of mutual friends by reducing later.
    val mutualFriendsDetails: RDD[(String, String)] = joinedRecords.map(row => {
      (row._2._1, row._2._2.fName + "\t" + row._2._2.lName + "\t" + row._2._2.address)
    })

    // Merging the details of the mutual friends.
    val mergedDetails: RDD[(String, String)] = mutualFriendsDetails.reduceByKey((details1, details2) => {
      details1 + "\t" + details2
    })

    // Formatting the output as asked in the question.
    val output: RDD[String] = mergedDetails.map(row => {
      val count = row._1.split(":")(1)
      count + "\t" + row._2
    })

    // Save the result to a text file.
    output.saveAsTextFile(outputPath)
//    output.collect().foreach(println)

  }

  def getMutual(line: String): Array[(String, Set[String])] = {
    val split: Array[String] = line.split("\\t")
    val myId: String = split(0)
//    if (split.length != 2) return
    val allFriends: String = if(split.length > 1) split(1) else "null"
    val listOfFriends: Array[String] = allFriends.split(",")

    // If input is => 1  2,3,4
    // then pairs value will be [(1, 2), (1, 3), (1, 4)]
    val pairs : Array[String] = listOfFriends.map(friendID => {
      if (myId < friendID) myId + "," + friendID else friendID + "," + myId
    })

    // After this transformation pairs array will be
    // [(1,2 2,3,4), (1,3 2,3,4), (1,4 2,3,4)]
    // Note: The RDD produced is PAIR RDD (a RDD having key/value pairs) in contrast to a NORMAL RDD)
    // A PAIR RDD has each element as (key, value) pair and can be formed y putting output values in ().
    pairs.map(pair => {
      (pair, listOfFriends.toSet)
    })
  }
}
