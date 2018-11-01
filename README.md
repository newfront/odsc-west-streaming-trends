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
+----------------------+----------+------------------+----------+-----+---------+--------+----------+
|country               |min(price)|avg(price)        |max(price)|total|varieties|wineries|prediction|
+----------------------+----------+------------------+----------+-----+---------+--------+----------+
|US                    |4         |24.869311655032252|41        |11317|176      |3186    |0         |
|France                |6         |21.427924528301887|41        |3975 |105      |1700    |0         |
|Italy                 |5         |22.84746778543602 |41        |3337 |130      |1493    |0         |
|Spain                 |4         |18.59246376811594 |41        |1725 |90       |819     |0         |
|Argentina             |5         |17.73173076923077 |40        |1040 |46       |329     |0         |
|Portugal              |6         |17.2031122031122  |40        |1221 |55       |259     |0         |
|Australia             |5         |20.46313799621928 |40        |529  |48       |244     |0         |
|Chile                 |6         |16.682076236820762|41        |1233 |45       |216     |0         |
|New Zealand           |8         |21.768571428571427|41        |350  |22       |171     |0         |
|Germany               |6         |22.517543859649123|41        |456  |24       |164     |0         |
|Austria               |9         |23.15336463223787 |41        |639  |35       |158     |0         |
|South Africa          |5         |19.05763688760807 |41        |347  |30       |154     |0         |
|Greece                |8         |20.74561403508772 |40        |114  |30       |48      |0         |
|Israel                |9         |23.06779661016949 |40        |118  |28       |29      |0         |
|Canada                |12        |26.065217391304348|40        |46   |18       |22      |0         |
|Hungary               |12        |22.25925925925926 |40        |27   |12       |18      |0         |
|Mexico                |10        |22.37037037037037 |40        |27   |15       |16      |0         |
|Bulgaria              |8         |13.5              |30        |44   |14       |15      |0         |
|Slovenia              |7         |18.96153846153846 |40        |26   |13       |15      |0         |
|Georgia               |10        |20.193548387096776|40        |31   |8        |14      |0         |
|Croatia               |13        |21.57894736842105 |38        |19   |11       |14      |0         |
|Uruguay               |10        |22.0              |40        |26   |9        |14      |0         |
|Romania               |4         |11.0              |22        |32   |11       |12      |0         |
|Turkey                |14        |23.8              |40        |20   |10       |9       |0         |
|Moldova               |8         |15.722222222222221|38        |18   |11       |7       |0         |
|Brazil                |12        |19.5              |31        |8    |6        |6       |0         |
|Lebanon               |14        |26.625            |40        |8    |4        |4       |0         |
|England               |25        |35.4              |40        |5    |2        |4       |0         |
|Serbia                |15        |23.0              |40        |5    |5        |3       |0         |
|Macedonia             |15        |15.0              |15        |8    |6        |3       |0         |
|Cyprus                |11        |16.25             |20        |4    |3        |3       |0         |
|Peru                  |14        |16.0              |17        |3    |3        |1       |0         |
|Morocco               |14        |18.0              |25        |6    |3        |1       |0         |
|India                 |10        |10.666666666666666|12        |3    |2        |1       |0         |
|Switzerland           |21        |21.0              |21        |1    |1        |1       |0         |
|Ukraine               |10        |10.0              |10        |1    |1        |1       |0         |
|Luxembourg            |16        |20.666666666666668|23        |3    |2        |1       |0         |
|Czech Republic        |15        |16.5              |18        |2    |2        |1       |0         |
|Bosnia and Herzegovina|13        |13.0              |13        |1    |1        |1       |0         |
+----------------------+----------+------------------+----------+-----+---------+--------+----------+

/* Empty due to not being in the RandomSplit test set... */
+-------+----------+----------+----------+-----+---------+--------+----------+  
|country|min(price)|avg(price)|max(price)|total|varieties|wineries|prediction|
+-------+----------+----------+----------+-----+---------+--------+----------+
+-------+----------+----------+----------+-----+---------+--------+----------+

+------------+----------+------------------+----------+-----+---------+--------+----------+
|country     |min(price)|avg(price)        |max(price)|total|varieties|wineries|prediction|
+------------+----------+------------------+----------+-----+---------+--------+----------+
|US          |41        |57.713103756708406|100       |4472 |76       |1491    |2         |
|Italy       |41        |62.29835831548893 |101       |1401 |63       |671     |2         |
|France      |41        |62.15113122171946 |101       |1105 |38       |495     |2         |
|Spain       |42        |62.053497942386834|100       |243  |31       |161     |2         |
|Portugal    |42        |63.58720930232558 |100       |172  |15       |95      |2         |
|Argentina   |41        |60.50819672131148 |100       |122  |15       |68      |2         |
|Austria     |41        |58.583333333333336|100       |156  |23       |60      |2         |
|Australia   |42        |66.78861788617886 |100       |123  |21       |59      |2         |
|Germany     |41        |59.94039735099338 |101       |151  |7        |54      |2         |
|Chile       |42        |67.38095238095238 |100       |84   |17       |52      |2         |
|New Zealand |41        |58.71153846153846 |90        |52   |5        |29      |2         |
|South Africa|42        |58.82051282051282 |100       |39   |13       |26      |2         |
|Israel      |42        |59.58620689655172 |100       |29   |7        |16      |2         |
|Canada      |45        |67.83333333333333 |95        |24   |10       |13      |2         |
|England     |42        |61.5625           |95        |16   |3        |12      |2         |
|Greece      |45        |51.285714285714285|59        |7    |4        |6       |2         |
|Hungary     |55        |66.66666666666667 |75        |3    |3        |3       |2         |
|Uruguay     |44        |46.333333333333336|50        |3    |2        |3       |2         |
|Brazil      |45        |52.5              |60        |2    |2        |2       |2         |
|Lebanon     |51        |63.0              |75        |3    |1        |2       |2         |
|Slovenia    |50        |55.0              |60        |2    |2        |2       |2         |
|Croatia     |57        |62.333333333333336|65        |3    |1        |2       |2         |
|Turkey      |45        |50.5              |56        |2    |2        |2       |2         |
|Mexico      |100       |100.0             |100       |1    |1        |1       |2         |
|Romania     |58        |58.0              |58        |1    |1        |1       |2         |
|Serbia      |42        |42.0              |42        |1    |1        |1       |2         |
+------------+----------+------------------+----------+-----+---------+--------+----------+

+------------+----------+------------------+----------+-----+---------+--------+----------+
|country     |min(price)|avg(price)        |max(price)|total|varieties|wineries|prediction|
+------------+----------+------------------+----------+-----+---------+--------+----------+
|US          |102       |143.9725085910653 |250       |291  |18       |171     |3         |
|France      |102       |140.88181818181818|249       |220  |20       |124     |3         |
|Italy       |102       |143.47619047619048|252       |168  |16       |110     |3         |
|Spain       |102       |146.6078431372549 |250       |51   |14       |36      |3         |
|Portugal    |110       |155.47619047619048|250       |21   |5        |16      |3         |
|Germany     |103       |149.8846153846154 |250       |26   |3        |15      |3         |
|Australia   |104       |148.04            |250       |25   |11       |15      |3         |
|Argentina   |110       |141.46153846153845|230       |26   |7        |13      |3         |
|Austria     |115       |121.5             |126       |4    |4        |4       |3         |
|New Zealand |120       |120.0             |120       |3    |1        |3       |3         |
|Chile       |120       |166.33333333333334|235       |6    |4        |3       |3         |
|South Africa|102       |120.5             |139       |2    |2        |2       |3         |
|Hungary     |118       |139.33333333333334|175       |3    |1        |2       |3         |
|Uruguay     |120       |120.0             |120       |1    |1        |1       |3         |
|Canada      |120       |120.0             |120       |1    |1        |1       |3         |
|Switzerland |160       |160.0             |160       |1    |1        |1       |3         |
+------------+----------+------------------+----------+-----+---------+--------+----------+

+------------+----------+------------------+----------+-----+---------+--------+----------+
|country     |min(price)|avg(price)        |max(price)|total|varieties|wineries|prediction|
+------------+----------+------------------+----------+-----+---------+--------+----------+
|France      |257       |358.10169491525426|550       |59   |5        |31      |4         |
|Italy       |255       |340.2             |540       |25   |8        |15      |4         |
|Germany     |279       |365.64285714285717|500       |14   |2        |11      |4         |
|US          |260       |333.6363636363636 |500       |11   |2        |9       |4         |
|Portugal    |275       |371.5             |495       |4    |3        |4       |4         |
|Spain       |300       |375.6666666666667 |500       |6    |3        |4       |4         |
|Chile       |260       |286.6666666666667 |300       |3    |2        |2       |4         |
|South Africa|275       |302.5             |330       |2    |2        |2       |4         |
|Australia   |300       |325.0             |350       |2    |2        |1       |4         |
|Romania     |320       |320.0             |320       |1    |1        |1       |4         |
|Hungary     |320       |320.0             |320       |1    |1        |1       |4         |
+------------+----------+------------------+----------+-----+---------+--------+----------+

+---------+----------+-----------------+----------+-----+---------+--------+----------+
|country  |min(price)|avg(price)       |max(price)|total|varieties|wineries|prediction|
+---------+----------+-----------------+----------+-----+---------+--------+----------+
|France   |569       |784.4285714285714|1500      |14   |5        |12      |5         |
|Portugal |770       |780.0            |790       |2    |1        |2       |5         |
|Germany  |775       |775.0            |775       |2    |1        |1       |5         |
|Australia|780       |780.0            |780       |1    |1        |1       |5         |
|Italy    |595       |595.0            |595       |1    |1        |1       |5         |
|Austria  |1100      |1100.0           |1100      |1    |1        |1       |5         |
+---------+----------+-----------------+----------+-----+---------+--------+----------+
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
