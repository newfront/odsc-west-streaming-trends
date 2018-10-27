import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrameReader

///////////////// NOTE: Open another terminal window (nc -lk 9999) FIRST OR THIS WON'T WORK /////////////////

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

// add coffeeStand
// would go in your main() function
//val sparkSession = SparkSession.builder.appName("coffeeShop").getOrCreate()

val sparkSession = spark

val availableCoffee = Seq(
  Coffee(name="folgers", roast=2, region="US", bean="robusta", acidity=7, bitterness=10, flavors=Seq("nutty")),
  Coffee(name="yuban", roast=2, region="Mexico", bean="robusta", acidity=6, bitterness=7, flavors=Seq("nutty")),
  Coffee(name="nespresso", roast=2, region="Cuba", bean="arabica", acidity=5, bitterness=3, flavors=Seq("nutty", "chocolate")),
  Coffee(name="ritual", roast=1, region="Brazil", bean="arabica", acidity=2, bitterness=1, flavors=Seq("fruity", "floral", "chocolate")),
  Coffee(name="four barrel", roast=1, region="Columbia", bean="arabica", flavors=Seq("nutty", "fruity")),
  Coffee(name="french collection", roast=3, region="France", bean="arabica", flavors=Seq("nutty", "fruity"))
  )

val coffeeStandDF = sparkSession.sparkContext.parallelize(availableCoffee, 3).toDF

import spark.implicits._


val coffeeRatingsReader = sparkSession.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
//coffeeRatingsReader.printSchema
// - value:String

val rawRatingsData: Dataset[String] = coffeeRatingsReader.as[String]

def asCoffeeRating(input: String): CoffeeRating = {
    val data = input.split(",")
    val coffeeName = data(0)
    val score = data(1).toInt
    val note = if (data.size > 2) Some(data(2)) else None
    CoffeeRating(coffeeName, score, note)
}

// convert the input
val coffeeRatingsInput = rawRatingsData.map { asCoffeeRating }.toDF

val coffeeAndRatingsDF = coffeeStandDF.join(coffeeRatingsInput, coffeeStandDF("name") === coffeeRatingsInput("coffeeName"))

val averageRatings = coffeeAndRatingsDF.groupBy(col("name")).agg(avg("score") as "rating").sort(desc("rating"))

val query = averageRatings.writeStream.outputMode("complete").format("console").start()

// nc -lk 9999
//folgers,1
//folgers,2,"gross"
//ritual,5,"awesome"

/*
-------------------------------------------                                     
Batch: 0
-------------------------------------------
+-------+------+
|   name|rating|
+-------+------+
|folgers|   1.0|
+-------+------+

-------------------------------------------                                     
Batch: 1
-------------------------------------------
+-------+------+
|   name|rating|
+-------+------+
|folgers|   1.5|
+-------+------+

-------------------------------------------                                     
Batch: 2
-------------------------------------------
+-------+------------------+
|   name|            rating|
+-------+------------------+
|folgers|2.6666666666666665|
+-------+------------------+
*/
