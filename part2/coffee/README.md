Running the Examples.
------------------------
Everyone loves coffee. Path to Spark Discovery through Coffee

1. spark-shell -i /path/to/basics.scala
2. spark-shell -i /path/to/dataframes.scala

Testing quick streaming

3. open up two terminal windows
 3a. in the first terminal window (nc -lk 9999)
 3b. in the second terminal window (spark-shell -i streaming_coffee.scala