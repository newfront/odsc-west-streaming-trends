/* spark is available via the command line in the spark-shell */
import org.apache.spark.sql.{SparkSession, DataFrame}
import spark.implicits._
import org.apache.spark.sql.functions._

val session: SparkSession = spark
val dataSetLocation = "./data/wine-reviews"
val WineReviewsCSV: DataFrame = session.read.option("inferSchema", "true").option("header","true").csv(s"$dataSetLocation/winemag-data-130k-v2.csv")

/* WineReviewsCSV.printSchema */
/*
root
 |-- _c0: string (nullable = true)
 |-- country: string (nullable = true)
 |-- description: string (nullable = true)
 |-- designation: string (nullable = true)
 |-- points: string (nullable = true)
 |-- price: string (nullable = true)
 |-- province: string (nullable = true)
 |-- region_1: string (nullable = true)
 |-- region_2: string (nullable = true)
 |-- taster_name: string (nullable = true)
 |-- taster_twitter_handle: string (nullable = true)
 |-- title: string (nullable = true)
 |-- variety: string (nullable = true)
 |-- winery: string (nullable = true)
 */

WineReviewsCSV.createOrReplaceTempView("reviews")
val twitterHandles = session.sql("select taster_name, taster_twitter_handle as twitter_handle from reviews where taster_twitter_handle IS NOT NULL")
val countByReviewer = twitterHandles.groupBy("twitter_handle").agg(count("twitter_handle") as "reviews")
countByReviewer.sort(desc("reviews")).show(10, false)