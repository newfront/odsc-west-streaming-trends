import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, Column}
import org.apache.spark.sql.types._
import java.io.Serializable

// SparkML Imports
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler

object WineUtils extends Serializable {
	def loadData(path: String): DataFrame = {
		spark.read.option("inferSchema", "false").option("inferSchema", "true").json(path)
	}

	def tastingNotes(df: DataFrame): String = {
		// can mess around easily with different selection criteria by changing the minSupport and minConfidence coefficents
		//val fpg = new FPGrowth().setItemsCol("items").setMinSupport(0.25).setMinConfidence(0.6)
		val fpg = new FPGrowth().setItemsCol("items").setMinSupport(0.05).setMinConfidence(0.6)
		val remover = new StopWordsRemover().setInputCol("items").setOutputCol("filteredItems")
		
		// Cleaning up the Wine Descriptions
		val descriptions = df.select(col("description")).where(col("description").isNotNull).map { case Row(s:String) => s.replace(",","").replace(".","").split(" ").toSet.toSeq }.toDF("items")
		// remove StopWords
		val filteredDescriptions = remover.transform(descriptions)
		val stopWordsFiltered = filteredDescriptions.select("filteredItems").toDF("items")
		val model = fpg.fit(stopWordsFiltered)
		val freqItems = model.freqItemsets.sort(desc("freq"))
    	val notes = freqItems.select("items").where(col("freq")>400)
		val topWords = notes.flatMap { case Row(notes: Seq[String]) => notes }.groupBy("value").count().sort(desc("count"))
		val tastingNotes = topWords.select("value").collect().map { case Row(s: String) => s }.toSeq.mkString(",")
		tastingNotes
	}

	def analyzeData(df: DataFrame): Unit = {
		val missingValues = df.schema.map { r => (r.name, df.where(col(r.name).isNull).count) }.toDF("name", "missing")
		missingValues.show
		/*
		+--------------------+-------+
		|                name|missing|
		+--------------------+-------+
		|             country|     63|
		|         description|      1|
		|         designation|  37462|
		|              points|     31|
		|               price|   9019|
		|            province|     66|
		|            region_1|  21241|
		|            region_2|  79454|
		|         taster_name|  26249|
		|taster_twitter_ha...|  31213|
		|               title|     10|
		|             variety|     12|
		|              winery|     12|
		+--------------------+-------+
		*/
		val distinctValues = df.schema.map { r =>
		(r.name, df
			.select(col(r.name))
			.where(col(r.name).isNotNull)
			.distinct.count)
		}.toDF("name", "distinct")

		distinctValues.show
		/*
		+--------------------+--------+
		|                name|distinct|
		+--------------------+--------+
		|             country|      43|
		|         description|  119955|
		|         designation|   37979|
		|              points|      21|
		|               price|     390|
		|            province|     425|
		|            region_1|    1229|
		|            region_2|      17|
		|         taster_name|      19|
		|taster_twitter_ha...|      15|
		|               title|  118840|
		|             variety|     707|
		|              winery|   16757|
		+--------------------+--------+
		*/
	}

	def priceByCountry(df: DataFrame): DataFrame = {
		// note: the where clause chaining off of isNotNull
		df.where(col("country").isNotNull.and(col("price").isNotNull)).groupBy("country").agg(min("price") as "min", avg("price") as "avg", max("price") as "max").sort(desc("max"))
	}

	def topTastingNotesByVariety(df: DataFrame): DataFrame = {
		// if there are 707 varieties of wine, and only 12 reviews missing varieties, then good candidate for topN wines - frequent items
		val totalReviewsByVariety = df.select(col("variety")).where(col("variety").isNotNull).groupBy(col("variety")).agg(count("*") as "total").sort(desc("total"))
		totalReviewsByVariety.show
		// To See the Spark Catalyst Plan for this query
		//totalReviewsByVariety.describe(true)
		//wineReviewsJson.cache
		//val totalReviewsByVarietyCached = wineReviewsJson.select(col("variety")).where(col("variety").isNotNull).groupBy(col("variety")).agg(count("*") as "total").sort(desc("total"))
		//totalReviewsByVarietyCached.describe(true)
		//totalReviewsByVarietyCached.limit(15).show
		/*
		+--------------------+-----+
		|             variety|total|
		+--------------------+-----+
		|          Pinot Noir|13272|
		|          Chardonnay|11753|
		|  Cabernet Sauvignon| 9472|
		|           Red Blend| 8946|
		|Bordeaux-style Re...| 6915|
		|            Riesling| 5189|
		|     Sauvignon Blanc| 4967|
		|               Syrah| 4142|
		|                Rosé| 3564|
		|              Merlot| 3102|
		|            Nebbiolo| 2804|
		|           Zinfandel| 2714|
		|          Sangiovese| 2707|
		|              Malbec| 2652|
		|      Portuguese Red| 2466|
		+--------------------+-----+
		*/
		val topVarieties = totalReviewsByVariety.select("variety").where(col("total")>2300)
		val varietyNames = topVarieties.collect().map { case Row(variety: String) => variety }.toSeq
		val tastingNotes = varietyNames.map { variety => (variety, WineUtils.tastingNotes(spark.sql(s"select * from winereviews where `variety` == '$variety'"))) }
		tastingNotes.toDF("variety", "tastingNotes")
	}

}

val dataSetLocation = "./data/wine-reviews"
val wineReviewsJsonLocation = s"$dataSetLocation/winemag-data-130k-v2.json"

val wineReviewsJson = WineUtils.loadData(wineReviewsJsonLocation)
wineReviewsJson.cache // cache off the data in memory vs going back to source json for each transaction

// understand a little more about the dataset
WineUtils.analyzeData(wineReviewsJson)

// Create Temporary Table (for the SQL fans out there)
wineReviewsJson.createOrReplaceTempView("winereviews")

// Can now start to explore tasting notes by Variety
// Take a look at Red Blends (I like them)
/*val redBlends = spark.sql("select * from winereviews where `variety` == 'Red Blend'")
redBlends.cache
redBlends.explain(true)
val redBlendTastingNotes = WineUtils.tastingNotes(redBlends)
*/

// Take a look at clustering the wines by price
// country has 43 unique values (which country has most expensive wine)
val priceRangeByCountry = WineUtils.priceByCountry(wineReviewsJson)
priceRangeByCountry.show(50, false)

val topVarietyNotes = WineUtils.topTastingNotesByVariety(wineReviewsJson)
//topVarietyNotes.show(20, false)
topVarietyNotes.foreach { row => println(s"Wine Variety: ${row.getString(0)}\nTasting Notes: ${row.getString(1)}\n") }

//val country_indexer = new StringIndexer().setInputCol("country").setOutputCol("country_index")
//val country_encoder = new OneHotEncoder().setInputCol("country_index").setOutputCol("country_encoded")

