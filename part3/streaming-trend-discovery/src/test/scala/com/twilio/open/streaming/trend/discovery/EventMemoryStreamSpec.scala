package com.twilio.open.streaming.trend.discovery

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.twilio.open.protocol.Calls.CallEvent
import com.twilio.open.streaming.trend.discovery.config.{AppConfig, AppConfiguration}
import com.twilio.open.streaming.trend.discovery.streams.EventAggregation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

class EventMemoryStreamSpec extends FunSuite with Matchers with SparkSqlTest {

  val log: Logger = LoggerFactory.getLogger(classOf[EventAggregation])
  private val pathToTestScenarios = "src/test/resources/scenarios"

  lazy val session: SparkSession = sparkSql

  override def conf: SparkConf = {
    new SparkConf()
      .setMaster("local[*]")
      .setAppName("aggregation-test-app")
      .set("spark.ui.enabled", "false")
      .set("spark.app.id", appID)
      .set("spark.driver.host", "localhost")
      .set("spark.sql.shuffle.partitions", "32")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memory", "1g")
      .set("spark.ui.enabled", "false")
      .setJars(SparkContext.jarOfClass(classOf[EventAggregation]).toList)
  }

  protected val checkpointDir: String = Files.createTempDirectory(appID).toString

  def appConfigForTest(): AppConfiguration = {
    val baseConfig = AppConfig("src/test/resources/app.yaml")

    baseConfig.copy(
      checkpointPath = checkpointDir
    )

    baseConfig
  }

  test("Should aggregate call events") {
    implicit val sqlContext: SQLContext = session.sqlContext
    import session.implicits._
    val appConfig = appConfigForTest()
    val scenario = TestHelper.loadScenario[CallEvent](s"$pathToTestScenarios/pdd_events.json")
    val scenarioIter = scenario.toIterator
    scenario.nonEmpty shouldBe true

    val trendDiscoveryApp = new TrendDiscoveryApp(appConfigForTest(), session)

    val kafkaData = MemoryStream[MockKafkaDataFrame]
    val processingTimeTrigger = Trigger.ProcessingTime(2.seconds)
    val eventAggregation = EventAggregation(appConfig)

    val processingStream = eventAggregation.process(kafkaData.toDF())(session)
      .writeStream
      .format("memory")
      .queryName("calleventaggs")
      .outputMode(eventAggregation.outputMode)
      .trigger(processingTimeTrigger)
      .start()


    // 22 events
    kafkaData.addData(scenarioIter.take(11).map(TestHelper.asMockKafkaDataFrame))
    processingStream.processAllAvailable()
    kafkaData.addData(scenarioIter.take(10).map(TestHelper.asMockKafkaDataFrame))
    processingStream.processAllAvailable()
    kafkaData.addData(scenarioIter.take(1).map(TestHelper.asMockKafkaDataFrame))
    processingStream.processAllAvailable()

    val df = session.sql("select * from calleventaggs")
    df.printSchema()
    df.show

    val res = session
      .sql("select avg(stats.p99) from calleventaggs")
      .collect()
      .map { r =>
        r.getAs[Double](0) }
      .head

    DiscoveryUtils.round(res) shouldEqual 7.56

    processingStream.stop()

  }



}
