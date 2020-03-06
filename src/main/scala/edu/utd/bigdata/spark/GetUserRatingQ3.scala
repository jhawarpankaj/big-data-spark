package edu.utd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GetUserRatingQ3 {

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
    case class Business(address: String)
    case class Review(userID: String, rating: String)

    val business = businessFile.map(line => {
      val split = line.split("::")
      val businessID = split(0)
      val address = split(1)
      (businessID, Business(address))
    })

    val review = reviewFile.map(line => {
      val split = line.split("::")
      val userID = split(1)
      val businessID = split(2)
      val rating = split(3)
      (businessID, Review(userID, rating))
    })

    val joined: RDD[(String, (Review, Business))] = review.join(business).distinct()

    val stanford = joined.filter({
      case (businessId, (review: Review, business: Business)) =>
        business.address.contains("Stanford")
    }).map({
      case (businessId, (review: Review, business: Business)) =>
        review.userID + "\t" + review.rating
    })

    // Write output to file...
    stanford.saveAsTextFile(outputPath)
    stanford.collect().foreach(println)
  }
}
