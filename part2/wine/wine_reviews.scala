import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._

val dataSetLocation = "./data/wine-reviews"
val wineReviewSchema = StructType(
	Array(
		StructField("_c0", StringType, true),
		StructField("country", StringType, true),
		StructField("description", StringType, true),
		StructField("designation", StringType, true),
		StructField("points",FloatType,true),
		StructField("price",FloatType,true),
		StructField("province", StringType, true),
		StructField("region_1",StringType,true),
		StructField("region_2",StringType,true),
		StructField("taster_name",StringType,true),
		StructField("taster_twitter_handle",StringType,true),
		StructField("title",StringType,true),
		StructField("variety",StringType,true),
		StructField("winery",StringType,true))
	)
val wine_reviews = spark.read.option("inferSchema", "false").option("header", "true").schema(wineReviewSchema).csv(s"$dataSetLocation/winemag-data-130k-v2.csv").toDF("id","country","description","designation","points","price","province","region1","region2","tasterName","tasterTwitterHandle","title","variety","winery")
val price_stats = wine_reviews.select("price").summary()

price_stats.printSchema
price_stats.show
/*
+-------+-----------------+
|summary|            price|
+-------+-----------------+
|  count|           120956|
|   mean|35.36592645259433|
| stddev|41.02276084008636|
|    min|              4.0|
|    25%|             17.0|
|    50%|             25.0|
|    75%|             42.0|
|    max|           3300.0|
+-------+-----------------+
*/


val missingDescriptions = wine_reviews.where(col("description").isNull).count // 1
val missingDesignations = wine_reviews.where(col("designation").isNull).count // 37462

// check out what is in the designation col...
wine_reviews.select(col("designation")).where(col("designation").isNotNull).show(10, false)
/*
+----------------------------------+
|designation                       |
+----------------------------------+
|Vulkà Bianco                      |
|Avidagos                          |
|Reserve Late Harvest              |
|Vintner's Reserve Wild Child Block|
|Ars In Vitro                      |
|Belsito                           |
|Shine                             |
|Les Natures                       |
|Mountain Cuvée                    |
|Rosso                             |
+----------------------------------+*/

// Manual Way to find all missing values in dataset
/*wine_reviews.where(col("id").isNull).count // 0
wine_reviews.where(col("country").isNull).count // 63
wine_reviews.where(col("description").isNull).count // 1
wine_reviews.where(col("designation").isNull).count // 37462
wine_reviews.where(col("points").isNull).count // 31
wine_reviews.where(col("price").isNull).count // 9019
wine_reviews.where(col("province").isNull).count // 66
wine_reviews.where(col("region1").isNull).count // 21241
wine_reviews.where(col("region2").isNull).count // 79454
wine_reviews.where(col("tasterName").isNull).count // 26249
wine_reviews.where(col("tasterTwitterHandle").isNull).count // 21213
wine_reviews.where(col("title").isNull).count // 10
wine_reviews.where(col("variety").isNull).count // 12
wine_reviews.where(col("winery").isNull).count // 12*/

// automatically look at the data quality
val missingValues = wine_reviews.schema.map { r => (r.name, wine_reviews.where(col(r.name).isNull).count) }.toDF("name", "missing")
missingValues.show(20, false)
/*
+-------------------+-------+
|name               |missing|
+-------------------+-------+
|id                 |0      |
|country            |63     |
|description        |1      |
|designation        |37462  |
|points             |31     |
|price              |9019   |
|province           |66     |
|region1            |21241  |
|region2            |79454  |
|tasterName         |26249  |
|tasterTwitterHandle|31213  |
|title              |10     |
|variety            |12     |
|winery             |12     |
+-------------------+-------+
*/

wine_reviews.select("title").show(10, false)
/*
+-----------------------------------------------------------------------------------+
|title                                                                              |
+-----------------------------------------------------------------------------------+
|Nicosia 2013 Vulkà Bianco  (Etna)                                                  |
|Quinta dos Avidagos 2011 Avidagos Red (Douro)                                      |
|Rainstorm 2013 Pinot Gris (Willamette Valley)                                      |
|St. Julian 2013 Reserve Late Harvest Riesling (Lake Michigan Shore)                |
|Sweet Cheeks 2012 Vintner's Reserve Wild Child Block Pinot Noir (Willamette Valley)|
|Tandem 2011 Ars In Vitro Tempranillo-Merlot (Navarra)                              |
|Terre di Giurfo 2013 Belsito Frappato (Vittoria)                                   |
|Trimbach 2012 Gewurztraminer (Alsace)                                              |
|Heinz Eifel 2013 Shine Gewürztraminer (Rheinhessen)                                |
|Jean-Baptiste Adam 2012 Les Natures Pinot Gris (Alsace)                            |
+-----------------------------------------------------------------------------------+
*/

wine_reviews.select("province").show(10, false)
/*
+-----------------+
|province         |
+-----------------+
|Sicily & Sardinia|
|Douro            |
|Oregon           |
|Michigan         |
|Oregon           |
|Northern Spain   |
|Sicily & Sardinia|
|Alsace           |
|Rheinhessen      |
|Alsace           |
+-----------------+
*/

// slow way
wine_reviews.count // 129975
val distinctProvinces = wine_reviews.select("province").distinct
val distinctWineries = wine_reviews.select("winery").where(col("winery").isNotNull).distinct
val distinctCountries = wine_reviews.select("country").where(col("country").isNotNull).distinct

// automatic discovery
val distinctValues = wine_reviews.schema.map { r => 
	(r.name, wine_reviews.select(col(r.name))
		.where(col(r.name).isNotNull).distinct.count) 
	}.toDF("name", "distinct")

distinctValues.show(20, false)
/*
+-------------------+--------+
|name               |distinct|
+-------------------+--------+
|id                 |129975  |
|country            |47      |
|description        |119958  |
|designation        |37995   |
|points             |23      |
|price              |390     |
|province           |449     |
|region1            |1252    |
|region2            |38      |
|tasterName         |35      |
|tasterTwitterHandle|32      |
|title              |118831  |
|variety            |727     |
|winery             |16774   |
+-------------------+--------+
*/

distinctCountries.show(48, false)
/*
+----------------------------------------------------------------------------------+
|country                                                                           |
+----------------------------------------------------------------------------------+
|Turkey                                                                            |
|Germany                                                                           |
| marked by tart citrus flavors that make it versatile with a wide range of foods."|
|France                                                                            |
|Greece                                                                            |
|null                                                                              |
|Slovakia                                                                          |
|Argentina                                                                         |
|Bordeaux-style Red Blend                                                          |
|Peru                                                                              |
|India                                                                             |
|China                                                                             |
|Chile                                                                             |
| fine and extremely polished; hold for 10 years."                                 |
|Croatia                                                                           |
|Italy                                                                             |
|90                                                                                |
|Spain                                                                             |
|US                                                                                |
|Morocco                                                                           |
|Ukraine                                                                           |
|Israel                                                                            |
|Cyprus                                                                            |
|Uruguay                                                                           |
|Mexico                                                                            |
|Georgia                                                                           |
|Armenia                                                                           |
|Switzerland                                                                       |
|Canada                                                                            |
|Macedonia                                                                         |
|Czech Republic                                                                    |
|Brazil                                                                            |
|Lebanon                                                                           |
|Slovenia                                                                          |
|Luxembourg                                                                        |
|New Zealand                                                                       |
|England                                                                           |
|Bosnia and Herzegovina                                                            |
|Portugal                                                                          |
|Australia                                                                         |
|Romania                                                                           |
|Bulgaria                                                                          |
|Austria                                                                           |
|Egypt                                                                             |
|Serbia                                                                            |
|South Africa                                                                      |
|Hungary                                                                           |
|Moldova                                                                           |
+----------------------------------------------------------------------------------+
*/
/*root
 |-- id: string (nullable = true)
 |-- country: string (nullable = true)
 |-- description: string (nullable = true)
 |-- designation: string (nullable = true)
 |-- points: float (nullable = true)
 |-- price: float (nullable = true)
 |-- province: string (nullable = true)
 |-- region1: string (nullable = true)
 |-- region2: string (nullable = true)
 |-- tasterName: string (nullable = true)
 |-- tasterTwitterHandle: string (nullable = true)
 |-- title: string (nullable = true)
 |-- variety: string (nullable = true)
 |-- winery: string (nullable = true)*/

val reviews = wine_reviews.na.fill(25.0f, Seq("price"))

// working with CSV can be difficult. Moving on to JSON...