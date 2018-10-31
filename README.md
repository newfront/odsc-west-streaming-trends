### AUTOMATING TREND DISCOVERY ON STREAMING DATASETS WITH SPARK 2.3
All Data, Relevant Information, Scripts, and Applications for the Open Data Science Conference (2018)

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
3a. `nc -lk 9999`
3b. `spark-shell -i part2/streaming_coffee.scala`

##### Streaming Aggregations on Coffee Ratings
~~~
folgers,1
folgers,2,"gross"
ritual,5,"awesome"
four barrel,5,"great"
four barrel,5,"great stuff"
four barrel,5,"really great stuff"
~~~

#### Playing with Wine
1. `cd data/winereviews && unzip winemag-csv.zip && unzip winemag-json.zip`
2. `spark-shell -i part2/wine/hello-wine.scala`
3. `spark-shell -i part2/wine/wine_reviews.scala`
3. `spark-shell -i part2/wine/wine_reviews_json.scala`


