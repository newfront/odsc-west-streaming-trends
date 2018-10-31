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
import org.apache.spark.ml.clustering.KMeans

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

// load reviews, cast points from String to Double for stats, drop original points and rename
val wineReviewsJson = WineUtils.loadData(wineReviewsJsonLocation).withColumn("num_points", 'points.cast("Double")).drop("points").withColumnRenamed("num_points", "points")

wineReviewsJson.cache // cache off the data in memory vs going back to source json for each transaction

// understand a little more about the dataset
WineUtils.analyzeData(wineReviewsJson)

// Create Temporary Table (for the SQL fans out there)
wineReviewsJson.createOrReplaceTempView("winereviews")

val priceStats = spark.sql("select min(price) as min, avg(price) as avg, percentile_approx(price, 0.1) as p10, percentile_approx(price, 0.25) as p25, percentile_approx(price, 0.5) as median, percentile_approx(price, 0.75) as p75, percentile_approx(price, 0.9) as p90, percentile_approx(price, 0.95) as p95, percentile_approx(price, 0.99) as p99, percentile_approx(price, 0.999) as p999, max(price) as max from winereviews")
priceStats.show
/*+---+------------------+---+---+------+---+---+---+---+----+----+
|min|               avg|p10|p25|median|p75|p90|p95|p99|p999| max|
+---+------------------+---+---+------+---+---+---+---+----+----+
|  4|35.363389129985535| 12| 17|    25| 42| 65| 85|155| 460|3300|
+---+------------------+---+---+------+---+---+---+---+----+----+*/

wineReviewsJson.describe().show
/*+-------+---------+--------------------+--------------------+------------------+--------+------------+-----------------+------------------+---------------------+--------------------+--------+--------+------------------+
|summary|  country|         description|         designation|             price|province|    region_1|         region_2|       taster_name|taster_twitter_handle|               title| variety|  winery|            points|
+-------+---------+--------------------+--------------------+------------------+--------+------------+-----------------+------------------+---------------------+--------------------+--------+--------+------------------+
|  count|   129908|              129971|               92506|            120975|  129908|      108724|            50511|            103727|                98758|              129971|  129970|  129971|            129971|
|   mean|     null|                null|  1494.4644378698224|35.363389129985535|    null|        null|             null|              null|                 null|                null|    null|Infinity| 88.44713820775404|
| stddev|     null|                null|    7115.55431803001|41.022217668087315|    null|        null|             null|              null|                 null|                null|    null|     NaN|3.0397302029160067|
|    min|Argentina|"Chremisa," the a...|#19 Phantom Limb ...|                 4|  Achaia|     Abruzzo| California Other|Alexander Peartree|          @AnneInVino|1+1=3 2008 Rosé C...|Abouriou|   1+1=3|              80.0|
|    max|  Uruguay|“Wow” is the firs...|                 “P”|              3300|    Župa|Zonda Valley|Willamette Valley|    Virginie Boone|       @worldwineguys|Štoka 2011 Izbran...| Žilavka|   Štoka|             100.0|
+-------+---------+--------------------+--------------------+------------------+--------+------------+-----------------+------------------+---------------------+--------------------+--------+--------+------------------+*/

wineReviewsJson.select("price").summary().show
/*+-------+------------------+                                                    
|summary|             price|
+-------+------------------+
|  count|            120975|
|   mean|35.363389129985535|
| stddev|41.022217668087315|
|    min|                 4|
|    25%|                17|
|    50%|                25|
|    75%|                42|
|    max|              3300|
+-------+------------------+*/

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

// wine point ranges [min:80, max:100], 80-84, 85-89, 90-94, 95-100
val bucketing = wineReviewsJson.where(col("price").isNotNull.and(col("points").isNotNull.and(col("country").isNotNull.and(col("variety").isNotNull)))).withColumn("quality", when(col("points") < 85, 0).when(col("points") < 90, 1).when(col("points") < 95, 2).otherwise(3))
//wineReviewsJson.stat.cov("price", "points") // 47.548
//bucketing.stat.cov("price", "quality") // 9.245
//bucketing.stat.cov("points", "quality") // 1.829

// indexers can't handle null values
val indexerCountry = new StringIndexer().setInputCol("country").setOutputCol("country_index")
val encoderCountry = new OneHotEncoder().setInputCol("country_index").setOutputCol("country_encoded")

val indexerVariety = new StringIndexer().setInputCol("variety").setOutputCol("variety_index")
val encoderVariety = new OneHotEncoder().setInputCol("variety_index").setOutputCol("variety_encoded")

val wineVectorAssembler = new VectorAssembler().setInputCols(Array("country_encoded", "variety_encoded", "price", "points", "quality")).setOutputCol("features")

val transformerPipeline = new Pipeline().setStages(Array(indexerCountry, encoderCountry, indexerVariety, encoderVariety, wineVectorAssembler))

val fittedPipeline = transformerPipeline.fit(bucketing)

val Array(trainingData, testData) = bucketing.randomSplit(Array(0.7, 0.3))

val transformedTraining = fittedPipeline.transform(trainingData)
transformedTraining.cache()

val kmeans = new KMeans().setK(6).setSeed(1L)
val kmModel = kmeans.fit(transformedTraining)

kmModel.computeCost(transformedTraining)

val transformedTest = fittedPipeline.transform(testData)
transformedTest.cache()
kmModel.computeCost(transformedTest)

val classified = kmModel.transform(transformedTest)

classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(0)).show(1000)
classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(1)).show(1000)
classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(2)).show(1000)
classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(3)).show(1000)
classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(4)).show(1000)
classified.select("country","price","winery","variety","title","features","prediction").where(col("prediction").equalTo(5)).show(1000)
