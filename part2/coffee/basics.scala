//spark-shell -i basics.scala 
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._

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

val availableCoffee = Seq(
  Coffee(name="folgers", roast=2, region="US", bean="robusta", acidity=7, bitterness=10, flavors=Seq("nutty")),
  Coffee(name="yuban", roast=2, region="Mexico", bean="robusta", acidity=6, bitterness=7, flavors=Seq("nutty")),
  Coffee(name="nespresso", roast=2, region="Cuba", bean="arabica", acidity=5, bitterness=3, flavors=Seq("nutty", "chocolate")),
  Coffee(name="ritual", roast=1, region="Brazil", bean="arabica", acidity=2, bitterness=1, flavors=Seq("fruity", "floral", "chocolate")),
  Coffee(name="four barrel", roast=1, region="Columbia", bean="arabica", flavors=Seq("nutty", "fruity"))
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

/*
val coffeeStand = spark.createDataset(availableCoffee)
val boldToLight = coffeeStand.sort(asc("roast"))
val boldMedium = boldToLight.where(col("roast")>1)
*/

// take the available coffee and add it to the stand
val coffeeStand = spark.createDataset(availableCoffee)
val coffeeRatings = spark.createDataset(rawCoffeeRatings)
val coffeeWithRatings = coffeeStand.join(coffeeRatings, coffeeStand("name") === coffeeRatings("coffeeName")).drop("coffeeName")

val sparkWay = coffeeWithRatings.groupBy("name").agg(avg("score") as "rating").sort(desc("rating"))

// create memory sql table
coffeeWithRatings.createOrReplaceTempView("coffee_ratings")
val sqlWay = spark.sql("select name, avg(score) as rating from coffee_ratings GROUP BY name ORDER BY rating DESC")

sparkWay.explain(true)
sparkWay.show(10, false)

sqlWay.explain(true)
sqlWay.show(10, false)