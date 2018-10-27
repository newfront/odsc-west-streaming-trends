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
// working with CSV can be difficult

// working with JSON
val wineReviewsJson = spark.read.option("inferSchema","true").json(s"$dataSetLocation/winemag-data-130k-v2.json")
val missingValues = wineReviewsJson.schema.map { r => (r.name, wine_reviews.where(col(r.name).isNull).count) }.toDF("name", "missing")
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

val distinctValues = wineReviewsJson.schema.map { r =>
	(r.name, wineReviewsJson
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

// Cleaning up the Wine Descriptions
val descriptions = wineReviewsJson.select(col("description")).where(col("description").isNotNull)
val wineDescriptions = descriptions.map { r => r.getString(0).replace(",","").replace(".","").split(" ").toSet.toSeq }.toDF("items")

val remover = new StopWordsRemover().setInputCol("items").setOutputCol("filteredItems")
val filteredDescriptions = remover.transform(wineDescriptions)
val stopWordsFiltered = filteredDescriptions.select("filteredItems").toDF("items")

//val fpg = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)
val fpg = new FPGrowth().setItemsCol("items").setMinSupport(0.05).setMinConfidence(0.6)
val model = fpg.fit(stopWordsFiltered)
val freqItems = model.freqItemsets
freqItems.show(100, false)
/*
+-------------------------+-----+                                               
|items                    |freq |
+-------------------------+-----+
|[citrus]                 |10594|
|[cherry]                 |25384|
|[cherry, fruit]          |7030 |
|[cherry, finish]         |6768 |
|[cherry, aromas]         |9015 |
|[cherry, tannins]        |10132|
|[cherry, flavors]        |11462|
|[cherry, palate]         |10072|
|[cherry, wine]           |9939 |
|[full]                   |8849 |
|[Sauvignon]              |7349 |
|[dry]                    |13627|
|[dry, flavors]           |7245 |
|[dry, wine]              |7052 |
|[pepper]                 |9188 |
|[aromas]                 |34963|
|[aromas, fruit]          |10201|
|[aromas, flavors]        |18416|
|[aromas, palate]         |17206|
|[aromas, palate, flavors]|7514 |
|[aromas, wine]           |11533|
|[rich]                   |15452|
|[rich, flavors]          |7076 |
|[rich, wine]             |9785 |
|[chocolate]              |7916 |
|[crisp]                  |11537|
|[crisp, wine]            |6788 |
|[bright]                 |9582 |
|[good]                   |8257 |
|[spice]                  |17711|
|[spice, aromas]          |6612 |
|[spice, flavors]         |7680 |
|[spice, wine]            |7956 |
|[offers]                 |12336|
|[offers, palate]         |6536 |
|[wine]                   |62802|
|[juicy]                  |8987 |
|[lemon]                  |7642 |
|[oak]                    |15007|
|[oak, flavors]           |7332 |
|[apple]                  |11467|
|[finish]                 |34569|
|[finish, fruit]          |9797 |
|[finish, aromas]         |13942|
|[finish, aromas, flavors]|9009 |
|[finish, aromas, palate] |6730 |
|[finish, flavors]        |18001|
|[finish, palate]         |13315|
|[finish, palate, flavors]|6522 |
|[finish, wine]           |11267|
|[green]                  |8084 |
|[notes]                  |17502|
|[notes, finish]          |7053 |
|[notes, flavors]         |6980 |
|[notes, palate]          |7139 |
|[texture]                |12175|
|[texture, wine]          |8126 |
|[dark]                   |9479 |
|[flavors]                |57207|
|[flavors, wine]          |25338|
|[raspberry]              |8820 |
|[ripe]                   |23504|
|[ripe, fruit]            |8330 |
|[ripe, tannins]          |7113 |
|[ripe, flavors]          |9545 |
|[ripe, acidity]          |7385 |
|[ripe, wine]             |13562|
|[years]                  |7331 |
|[fruits]                 |12879|
|[fruits, wine]           |9054 |
|[shows]                  |10336|
|[peach]                  |8000 |
|[soft]                   |11761|
|[soft, wine]             |6757 |
|[drink]                  |9478 |
|[drink, wine]            |6565 |
|[fruit]                  |40369|
|[fruit, flavors]         |15943|
|[fruit, flavors, wine]   |7817 |
|[fruit, wine]            |20359|
|[nose]                   |16642|
|[nose, flavors]          |6830 |
|[nose, palate]           |11331|
|[firm]                   |8298 |
|[pear]                   |7262 |
|[blend]                  |12809|
|[vanilla]                |10087|
|[black]                  |21095|
|[black, cherry]          |8874 |
|[black, fruit]           |7111 |
|[black, aromas]          |6859 |
|[black, tannins]         |9845 |
|[black, flavors]         |8171 |
|[black, palate]          |7996 |
|[black, wine]            |10027|
|[balanced]               |7469 |
|[fresh]                  |14700|
|[fresh, wine]            |7282 |
|[sweet]                  |11188|
|[fruity]                 |8925 |
+-------------------------+-----+
*/

/*
groupBy(variety,distinction) fit subset of review notes to figure out the tasting notes common to people
which tasting notes show up by price? region?
*/
