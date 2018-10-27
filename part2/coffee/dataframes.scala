//spark-shell -i dataframes.scala 
// :load /path/to/SparkTechTalk/dataframes.scala (from spark-shell)

case class Coffee(
  name: String,
  roast:Int,
  region:String,
  bean: String,
  acidity:Int = 1,
  bitterness:Int = 1,
  flavors: Seq[String]
  )

case class CoffeeRating(
  coffeeName: String,
  score: Int,
  notes: Option[String] = None
  )

import spark.implicits._

val availableCoffee = Seq(
  Coffee(name="folgers", roast=2, region="US", bean="robusta", acidity=7, bitterness=10, flavors=Seq("nutty")),
  Coffee(name="yuban", roast=2, region="Mexico", bean="robusta", acidity=6, bitterness=7, flavors=Seq("nutty")),
  Coffee(name="nespresso", roast=2, region="Cuba", bean="arabica", acidity=5, bitterness=3, flavors=Seq("nutty", "chocolate")),
  Coffee(name="ritual", roast=1, region="Brazil", bean="arabica", acidity=2, bitterness=1, flavors=Seq("fruity", "floral", "chocolate")),
  Coffee(name="four barrel", roast=1, region="Columbia", bean="arabica", flavors=Seq("nutty", "fruity")),
  Coffee(name="french collection", roast=3, region="France", bean="arabica", flavors=Seq("nutty", "fruity"))
  )

// take the available coffee and add it to the stand
val coffeeStand = spark.sparkContext.parallelize(availableCoffee, 3)

// convert from RDDs to DataFrames
val coffeeStandDF = coffeeStand.toDF
//coffeeStandDF.printSchema

val boldToLight = coffeeStandDF.sort(desc("roast"))
// boldToLight.show(20, false)

val boldMedium = boldToLight.where(col("roast") > 1)
boldMedium.show

case class CoffeeRating(
  coffeeName: String,
  score: Int,
  notes: Option[String] = None
  )

val rawCoffeeRatings = Seq(
  CoffeeRating("folgers",1,Some("terrible")),
  CoffeeRating("folgers",2,Some("meh")),
  CoffeeRating("yuban",3,Some("worth the money")),
  CoffeeRating("nespresso",2,Some("it's coffee")),
  CoffeeRating("ritual",5,Some("fantastic")),
  CoffeeRating("four barrel",3),
  CoffeeRating("four barrel",5,Some("my fav")),
  CoffeeRating("ritual",4)
  )

val coffeeRatings = spark.sparkContext.parallelize(rawCoffeeRatings, 2)
val coffeeRatingsDF = coffeeRatings.toDF

// join coffee and ratings Data Frames
val coffeeAndRatingsDF = coffeeStandDF.join(coffeeRatingsDF, coffeeStandDF("name") === coffeeRatingsDF("coffeeName"))

val averageRatings = coffeeAndRatingsDF.groupBy(col("name")).agg(avg("score") as "rating").sort(desc("rating"))
//averageRatings.show
// explain how spark will execute the given function
//averageRatings.explain

// create a temp sql view to show how to use sql to land at the same approach
coffeeAndRatingsDF.createOrReplaceTempView("coffee_ratings")

val averageRatingsViaSparkSQL = spark.sql("select name, avg(score) as rating from coffee_ratings GROUP BY name ORDER BY rating DESC")
// averageRatingsViaSparkSQL.show

import org.apache.spark.sql.functions._

