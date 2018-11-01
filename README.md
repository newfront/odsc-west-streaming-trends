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


