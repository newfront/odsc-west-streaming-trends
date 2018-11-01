### AUTOMATING TREND DISCOVERY ON STREAMING DATASETS WITH SPARK 2.3
All Data, Relevant Information, Scripts, and Applications for the Open Data Science Conference (2018)

#### The Speaker
**Scott Haines**
I work at [Twilio](https://www.twilio.com/)

Places I exist online
* [Github - newfront](https://github.com/newfront)
* [Medium](https://medium.com/@newfrontcreative)
* [Twitter](https://twitter.com/newfront)
* [LinkedIn](https://www.linkedin.com/in/scotthaines/)

#### Getting Started
1. Download and Install Spark (http://spark.apache.org/downloads.html) or https://www.apache.org/dyn/closer.lua/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz 

2. Have maven 3 installed. `brew install maven3` if you want to build the Streaming Trend Discovery code

#### Data Set Information
[Wine Reviews](https://www.kaggle.com/zynicide/wine-reviews) - Thanks to zynicide and kaggle.com for the data set.

### Running the Code Examples
All actions should be run from root of the odsc directory

#### Playing with Coffee
1. `spark-shell -i part2/coffee/basics.scala`
2. `spark-shell -i part2/coffee/dataframes.scala`

3. Requires 2 terminal windows
* 3a. `nc -lk 9999`
* 3b. `spark-shell -i part2/streaming_coffee.scala`

##### Streaming Aggregations on Coffee Ratings
Now in the terminal window (nc -lk 9999) just copy and paste each of the following lines. nc -lk takes stdin and spark will pick up from that socket connection.
~~~
folgers,1
folgers,2,"gross"
ritual,5,"awesome"
four barrel,5,"great"
four barrel,5,"great stuff"
four barrel,5,"really great stuff"
~~~

In the spark streaming coffee terminal you should see the following
~~~
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
~~~

#### Playing with Wine
1. `cd data/winereviews && unzip winemag-csv.zip && unzip winemag-json.zip`
2. `spark-shell -i part2/wine/hello-wine.scala`
3. `spark-shell -i part2/wine/wine_reviews.scala`
3. `spark-shell -i part2/wine/wine_reviews_json.scala`

#### Lessons Learned
* Wine Reviews JSON data is easier and better to work with
* SparkML **StopWordsRemover** allows us to quickly remove common words from the wine reviews
* SparkML **FPGrowth** allows us to quickly generate Frequent Item Lists using the [Apriori Algorithm](https://en.wikipedia.org/wiki/Apriori_algorithm)
* Generating Tasting Notes from Wine can be easy with a little trial and error
~~~scala
def tastingNotes(df: DataFrame): String = {
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
~~~

Output when FPGrowth is run on topN varieties in the corpus of Wine Reviews
~~~
Wine Variety: Pinot Noir
Tasting Notes: wine,flavors,cherry,fruit,Pinot,acidity,tannins,palate,raspberry,Noir,red,finish,ripe,oak,black,cola,aromas,Drink,spice,rich,dry,silky,texture,light,years,soft,fruits,nose,plum,structure,strawberry,juicy,character,complex,spicy,vanilla,vineyard,touch,new,earthy,cherries,bit,earth,cranberry,berry,fresh,dried,firm,flavor,dark,full,tea,age,sweet,notes,vintage,well,good,tart,drink,raspberries,crisp,shows,smoky,bottling,offers,bright

Wine Variety: Bordeaux-style Red Blend
Tasting Notes: wine,tannins,Cabernet,fruit,flavors,Merlot,Sauvignon,blend,ripe,fruits,Franc,acidity,black,Drink,rich,Petit,Verdot,juicy,structure,wood,firm,dry,currant,dark,well,aging,character,aromas,cherry,berry,blackberry,spice,structured,dense,drink,fruity,years,red,soft,still,ready,tannic,2017,fine,solid,2018,fresh,Malbec,Barrel,full,concentrated,age,attractive,texture,finish,sample,shows,balanced,core,balance,palate,oak,fruitiness,Bordeaux,chocolate,smooth,sweet,plum,notes,vintage,good,give,smoky,weight,also

Wine Variety: Riesling
Tasting Notes: flavors,palate,finish,Riesling,acidity,dry,wine,lemon,notes,peach,apple,fruit,nose,lime,aromas,fresh,long,sweet,citrus,ripe,Drink,juicy,honey,orange,green,refreshing,off-dry,stone,apricot,mineral,pear,grapefruit,white,zesty,freshness,concentrated,minerality,tangerine,yet,style,tart,crisp,fruity

Wine Variety: Sauvignon Blanc
Tasting Notes: flavors,wine,finish,aromas,palate,acidity,Blanc,Sauvignon,green,citrus,fruit,crisp,grapefruit,apple,fresh,lime,nose,texture,ripe,Drink,rich,tropical,dry,white,lemon,fruits,pineapple,clean,notes,melon,peach,drink

Wine Variety: Syrah
Tasting Notes: flavors,wine,Syrah,fruit,black,aromas,pepper,finish,tannins,palate,cherry,blackberry,acidity,Drink,meat,rich,berry,dark,oak,spice,chocolate,nose,plum,notes,shows,ripe
~~~

#### KMeans Clustering in WineReviews
After exploring the data, seeing what is missing, and which values have the least number of distinct values (low cardinality), and after looking at auto-generative Tasting Notes. It came time to look at how to use KMeans clustering of the Wine Reviews dataset.

The idea was this. Given that Wine Reviews have scores (points), but also have a price, country of origin, and varitey of wine I thought it may be fun to create a range based value called **quality** so the clustering algorithm would have an additional help. Creating a range based value is a simple way of applying dimensional reduction to continious variables. In this case, we create a step based range vs having all values from **80-100** so less effort when clustering.

First we removed all NULL values from the dataset. Then applied the `withColumn("quality",...)` to create our range buckets.
~~~
val bucketing = wineReviewsJson.where(col("price").isNotNull.and(col("points").isNotNull.and(col("country").isNotNull.and(col("variety").isNotNull)))).withColumn("quality", when(col("points") < 85, 0).when(col("points") < 90, 1).when(col("points") < 95, 2).otherwise(3))
~~~

The KMeans Clustering Code
~~~
def kmeansWine(df: DataFrame): DataFrame = {
		df.cache()
		// note: indexers can't handle null values
		val indexerCountry = new StringIndexer().setInputCol("country").setOutputCol("country_index")
		val encoderCountry = new OneHotEncoder().setInputCol("country_index").setOutputCol("country_encoded")
		val indexerVariety = new StringIndexer().setInputCol("variety").setOutputCol("variety_index")
		val encoderVariety = new OneHotEncoder().setInputCol("variety_index").setOutputCol("variety_encoded")
		val wineVectorAssembler = new VectorAssembler().setInputCols(Array("country_encoded", "variety_encoded", "price", "points", "quality")).setOutputCol("features")
		val transformerPipeline = new Pipeline().setStages(Array(indexerCountry, encoderCountry, indexerVariety, encoderVariety, wineVectorAssembler))
		val fittedPipeline = transformerPipeline.fit(df)
		val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
		val transformedTraining = fittedPipeline.transform(trainingData)
		transformedTraining.cache()
		val kmeans = new KMeans().setK(6).setSeed(1L)
		val kmModel = kmeans.fit(transformedTraining)
		kmModel.computeCost(transformedTraining)
		val transformedTest = fittedPipeline.transform(testData)
		transformedTest.cache()
		kmModel.computeCost(transformedTest)
		kmModel.transform(transformedTest)
	}
~~~

Applying this to WineReviews and looking at the end results
~~~
val wineClusters = WineUtils.kmeansWine(bucketing)
(0 to 5).map { i =>
    val cluster = wineClusters.select("country","price","points","winery","variety","title","features","prediction").where(col("prediction").equalTo(i))
	cluster
		.groupBy("country")
		.agg(
			min("price"),
			avg("price"),
			max("price"),
			count("*") as "total",
			countDistinct("variety") as "varieties",
			countDistinct("winery") as "wineries",
			lit(i) as "prediction"
		)
		.sort(desc("wineries"))
		.show(50, false)
}
~~~

Looking at the Clusters
~~~

~~~

#### Spark SQL Tricks
[Working with Apache Spark DataFrames, Json and the Good Ol StructType](https://medium.com/@newfrontcreative/working-with-apache-spark-dataframes-json-and-the-good-ol-structtype-6291bdcd44bd)

### Streaming Trend Discovery (part3)
Important Technologies and Concepts

#### Apache Kafka (reliable pub/sub infrastructure)
[Docs](https://kafka.apache.org/documentation/#uses)

Apache Kafka allows you to reliably read and more importantly (re-read) a stream of time series data. This is important given streaming applications can fail and having to re-run from the last state of an application may require re-reading data in order to pick things back up.

#### Data Sketches
[DataSketch library docs](https://datasketches.github.io/)
[Yahoo DataSketches Blog Post](https://yahooeng.tumblr.com/post/135390948446/data-sketches)
[T-Digest](http://koff.io/posts/using-t-digest/)
[Monoid Addition via T-Digest](http://erikerlandson.github.io/blog/2016/12/19/converging-monoid-addition-for-t-digest/)

Data Sketching has many functions in statistics, and with respect to Percentiles/Quantiles they make it very easy to approximate the actual quantiles data and understand the underlying density (histogram via Probability Density Function) and also understand the shape in terms of Cumulative Density. More importantly to their function in Spark is their *native ability to be distributed and mergable* which means that distributed statistics (monoid / monadic systems idea) becomes as simple as running on your localhost/laptop.

#### Windowing and Watermarking Data
**Windowing** data is a concept when dealing with TimeSeries Data. A Window is a logical subset of a continuous data stream that begins and ends at specific points in time.

**Watermarking** data is a pattern that has become more popular in the streaming world given that Upstream systems may decide that they needed to replay data in order to fulfill an expectation and unfortunately your data stream may have mixed "times" due to the replay. So when you apply a Watermark to your data you are denoting when you would like to reject and ignore **late arriving data**.

#### Simple Trick for Dimensional reduction in your Metric Streams.

Say you have the following two events
~~~
  {
    "id": "uuid1",
    "type": "GamePlay",
    "value": 26.01,
    "metric": "session_time",
    "dimensions": {
      "country": "US",
      "user": "id123",
      "game_id": "UUID"
    }
  },
  {
    "id": "uuid2",
    "type": "GamePlay",
    "value": 10.0,
    "metric": "game_load_time",
    "dimensions": {
      "country": "US",
      "user": "id123",
      "game_id": "UUID"
    }
  }
~~~

The Events (SessionTime and GameLoadTime) are just two different metrics sharing a similar pattern when it comes to the underlying dimensions (categorical features) of said events. Your **hashed dimensions** would end up being the common underlying dimensions that are not **unique** to any one common entity (user etc).

Hashing Code
~~~
def generateId(bytes: Array[Byte]): String = {
  val hf = Hashing.murmur3_128()
  val hc = hf.hashBytes(bytes)
  BaseEncoding.base64Url()
    .omitPadding()
    .encode(hc.asBytes())
}
~~~

Resulting in the following logical hash
~~~
generateId(s"country=$country:game_id=$game_id".getBytes) // A2SFbSnmugskJNwhhdLg6w
~~~

#### How Streaming Discovery Works

##### The Data
Data Structure must be known ahead of time and Dimensions "shouldn't" be fully freeform, due to the need to be able to generate a Dimensional Hash. The Hash of the common dimensions creates a latch point or intersection amoungst your metrics, and solves the problem of (what other metrics are experiencing this similar or disimilar behavior). You essentially are creating an ad-hoc sub-graph with respect to Time and Common Dimensions.

##### Mixing Streaming Applications
Given that we can read data and re-read data (from kafka) and given that Kafka can host multiple topics, then we use the following approach to automatically detecting trends in "real-ish time".

#### Discovery Engine Part 1
First we have our **validated and structured core events** (remember that everything that is enqueued to kafka must first have a valid Data Contract - this is the base hypothesis in order for this to work). This is the initial ingest flow in the discovery architecture. This application is projecting the Core Events into our binary compactible and serializable formats - eg. the `MetricAggregation` primitive and removing the hard work of storing potentially large overhead in memory to preserve the full set of metrics.

This application can be configured with any variable **Window**, but it is important that this window be small enough so you can **store the aggregate data in memory**. This is the most important phase of the engine given that this must **be correct** and running at all times (otherwise you break rules of Real-Time accountability)

Example of the Streaming Query
~~~
val aggregationOutputStream = EventAggregation(config).process(readKafkaStream())(spark)
  .writeStream
  .queryName("streaming.trend.discovery")
  .outputMode(eventProcessor.outputMode)
  .trigger(Trigger.ProcessingTime(Duration(config.triggerInterval)))
  .format("kafka")
  .option("topic", "spark.summit.call.aggregations")
  .options(Map(
    "checkpointLocation" -> config.checkpointPath
  ))
  .start()
~~~

It is worth noting that this application is setup to use `EventTime` vs `ProcessingTime`. This is very important because EventTime means you trust the timestamp of a given event (or you have added your own **logged_event_ts**). This allows you to do rapid replay and recovery. Consider this.

##### Processing Time
You have a streaming application and it is Windowing by 1 hour using processing time. It fails. So it takes 1 hour from the time of failure to recover.

##### Event Time
You have a streaming application and it is Windowing by 1 hour using event time. It fails. So you restart it and could be caught up in a matter of seconds (depends on the spark cluster size and capabilities...)

#### Discovery Engine Part 2
Given that you have the base pattern (eg. `streaming-trend-discovery` app) for doing Performance Optimized, Windowed, Recoverable, Monitored, Unit Tested streaming aggregation. Then you can easily make a slight update to the code base to handle the ingestion of data from a second Kafka Topic. This application will be dead simple. Here is the gist.

1. You want to collect up to N windowed aggregates to do a delta stream analysis.
2. Given you are using **EventTime** processing and have say an aggregate window of 5 minutes (from Part 1)
3. Then you could create a delta stream that take the last 4 windows (eg. 20m) - to do comparison of the data and discover treands in your metric aggregation stream. Given you have a strong hash (**dimensional_hash**) you can use this potentially as a secondary grouping key and then simply window and aggregate etc! 

Output Format from the Example Application

**TimeSeries Rows**
~~~
+------+-------------+-------------+---------------+-------+--------------------+--------------------+--------------------+--------------------+
|metric| window_start|   window_end|window_interval|samples|               stats|           histogram|          dimensions|      dimension_hash|
+------+-------------+-------------+---------------+-------+--------------------+--------------------+--------------------+--------------------+
|   pdd|1527966000000|1527966300000|             5m|     11|[0.7, 1.1, 2.2, 3...|[0.0, 0.0, 0.0, 2...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527967200000|1527967500000|             5m|      1|[5.9, 5.9, 5.9, 5...|[0.0, 0.0, 0.0, 0...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527966000000|1527966300000|             5m|     12|[0.7, 1.7, 2.2, 3...|[0.0, 0.0, 0.0, 2...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527966300000|1527966600000|             5m|      6|[1.1, 1.3, 1.9, 2...|[0.0, 0.0, 0.0, 0...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527966600000|1527966900000|             5m|      2|[1.9, 1.9, 1.9, 1...|[0.0, 0.0, 0.0, 0...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527966000000|1527966300000|             5m|     12|[0.7, 1.7, 2.2, 3...|[0.0, 0.0, 0.0, 2...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
|   pdd|1527968400000|1527968700000|             5m|      1|[10.9, 10.9, 10.9...|[0.0, 0.0, 0.0, 0...|[us, outbound, te...|F-WIKm9w3XXTLzjJb...|
+------+-------------+-------------+---------------+-------+--------------------+--------------------+--------------------+--------------------+
~~~

**StructType**
~~~
|-- metric: string (nullable = true)
 |-- window_start: long (nullable = true)
 |-- window_end: long (nullable = true)
 |-- window_interval: string (nullable = true)
 |-- samples: integer (nullable = true)
 |-- stats: struct (nullable = true)
 |    |-- min: double (nullable = true)
 |    |-- p25: double (nullable = true)
 |    |-- median: double (nullable = true)
 |    |-- p75: double (nullable = true)
 |    |-- p90: double (nullable = true)
 |    |-- p95: double (nullable = true)
 |    |-- p99: double (nullable = true)
 |    |-- max: double (nullable = true)
 |    |-- mean: double (nullable = true)
 |    |-- sd: double (nullable = true)
 |    |-- variance: double (nullable = true)
 |-- histogram: struct (nullable = true)
 |    |-- bin1: double (nullable = true)
 |    |-- bin2: double (nullable = true)
 |    |-- bin3: double (nullable = true)
 |    |-- bin4: double (nullable = true)
 |    |-- bin5: double (nullable = true)
 |    |-- bin6: double (nullable = true)
 |-- dimensions: struct (nullable = true)
 |    |-- country: string (nullable = true)
 |    |-- direction: string (nullable = true)
 |    |-- carrier: string (nullable = true)
 |    |-- route: string (nullable = true)
 |-- dimension_hash: string (nullable = true)
~~~

Example of all that is needed (assuming you are using Protobuf and the Encoder from the example application)
~~~
aggregateWindowStream
  .withWatermark("window_end", "5 minutes")
  .groupBy("metric", "dimensional_hash", window($"window_end", "20 minutes"))
  .agg(
    avg("stats.avg") as "avg_avg",
    avg("stats.median") as "avg_median",
    min("stats.p95") as "min_p95",
    avg("stats.p95") as "avg_p95",
    max("stats.p95") as "max_p95",
    min("stats.sd") as "min_sd",
    avg("stats.sd") as "avg_sd",
    max("stats.sd") as "max_sd",
    min("samples") as "min_metrics",
    avg("samples") as "avg_metrics",
    max("samples") as "max_metrics"
  )
~~~

The above would create a stream of aggregates (see Part 1) and then watermark and window these aggregates (further aggregate), and then it is up to you to analyze this further using the tools that are now part of your Spark arsenal.

Thanks
