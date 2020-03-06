package edu.utd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top10BusinessQ4 {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Invalid input format. Required <business_file> <review_file> <output_path>")
      return
    }

    // Initialize job configs...
    val conf = new SparkConf().setAppName("GetUserRating").setMaster("local")
    val sc = new SparkContext(conf)

    // Read input files...
    val businessFile = sc.textFile(args(0))
    val reviewFile = sc.textFile(args(1))
    val outputPath = args(2)

    // case classes for business and review...
    case class Business(address: String, categories: String)
    case class Review(rating: Double)

    val business = businessFile.map(line => {
      val split = line.split("::")
      val businessID = split(0)
      val address = split(1)
      val categories = split(2)
      (businessID, Business(address, categories))
    })

    val review = reviewFile.map(line => {
      val split = line.split("::")
      val businessID = split(2)
      val rating = split(3).toDouble
      (businessID, rating)
    })

    val allAvg: RDD[(String, Double)] = review.groupByKey().map({
      case (businessID, ratings) => (businessID, ratings.sum / ratings.size)
    })
    .map(row => row.swap)
    .sortByKey(false, 1)
    .map(row => row.swap)
//    .distinct()
    allAvg.collect().foreach(println)

    // Extracting the top 10 from the entire list.
    val top10: RDD[(String, Double)] = sc.parallelize(allAvg.take(10))
    top10.collect().foreach(println)
    val result = top10.join(business).groupByKey().flatMap({
      case (businessID, iter) =>
        iter.map({
          case (rating, business: Business) =>
            (businessID, businessID + "\t" + business.address + "\t" +
              business.categories + "\t" + rating)
        })
    })

    val mergedDetails: RDD[String] = result.reduceByKey((details1, details2) => {
      details1
    }).map(row => row._2)

    mergedDetails.collect().foreach(println)
//    val allAvg = review.groupByKey().map({
//      case (businessID, rating) =>
//        val ratingList = List(rating)
//        val avg = ratingList.sum / ratingList.size
//        (businessID, avg)
//    })

//    allAvg.collect().foreach(println)

  }
}
