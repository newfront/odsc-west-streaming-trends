//spark-shell -i basics.scala 

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
  name: String,
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

// take the available coffee and add it to the stand
val coffeeStand = spark.sparkContext.parallelize(availableCoffee, 3)

// simple sorting - (bold -> light)
val boldToLight = coffeeStand.sortBy(_.roast, ascending=false)

// view the hierarchy of the RDD
//boldToLight.toDebugString

// simple filtering - (remove light roast)
val boldMedium = boldToLight.filter(_.roast > 1)
//boldMedium.toDebugString

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

// map to pair (name, Coffee)
val coffeeWithKey = coffeeStand.map { coffee => (coffee.name, coffee) }
// map to pair (name, CoffeeRating)
val coffeeRatingsWithKey = coffeeRatings.map { rating => (rating.name, rating) }
// join coffee with ratings by key
val joinedCoffeeAndRatings = coffeeWithKey.join(coffeeRatingsWithKey)

// map to (Coffee, CoffeeRating)
val coffeeRatingPair = joinedCoffeeAndRatings.map { _._2 }

def averageRating(ratings: Iterable[Double]): Double = {
  ratings.sum / ratings.size
}

// filter (Coffee, CoffeeRating) _._1 (Coffee) .name is coffee name
val folgersCoffeeRatings = coffeeRatingPair.filter { _._1.name == "folgers" }
val localFolgersRatings = folgersCoffeeRatings.collect.map { _._2 }
// import default executor (ForkJoinPool)
//import scala.concurrent.ExecutionContext.Implicits.global
// return FutureAction[Seq[(Coffee, CoffeeRating)]] - This will collect the records to the Driver
//val eventualFolgersRatings = folgersCoffeeRatings.collectAsync

val avgFolgersRating = averageRating(localFolgersRatings.map { _.score.toDouble })

/*
scala> folgersCoffeeRatings.toDebugString
res19: String =
(4) MapPartitionsRDD[45] at filter at <console>:25 []
 |  MapPartitionsRDD[43] at join at <console>:27 []
 |  MapPartitionsRDD[42] at join at <console>:27 []
 |  CoGroupedRDD[41] at join at <console>:27 []
 +-(4) MapPartitionsRDD[36] at map at <console>:25 []
 |  |  ParallelCollectionRDD[0] at parallelize at <console>:25 []
 +-(4) MapPartitionsRDD[37] at map at <console>:25 []
    |  ParallelCollectionRDD[28] at parallelize at <console>:25 []
 */

