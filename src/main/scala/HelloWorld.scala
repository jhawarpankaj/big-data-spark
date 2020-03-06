import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args : Array[String]) {
    if (args.length != 2) {
      println("Invalid input parameters. Required: <input_file> <output_path>")
      return
    }
    val conf = new SparkConf().setAppName("CountMutualFriends").setMaster("local")
    val sc = new SparkContext(conf)
    val inputFile = args(0)
    val outputFile = args(1)
    println("Input file: " + inputFile)
    val dataFile = sc.textFile(inputFile)
//    println("Type: " + dataFile.getClass.toString)
    val counts = dataFile.flatMap(Map)
//      val counts = dataFile.map(line => line.split("\\t")).map(word => (word, 10))
//    val counts = dataFile.flatMap(line => line.split(" ")).map(word => (word,1))
    counts.collect().foreach(println)
  }

  def Map(line: String) = {
    val line1 = line.split("\\t+")
    val person = line1(0)
    val newfriends = if (line1.length > 1) line1(1) else "null"

    val nfriends = newfriends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  }
}


