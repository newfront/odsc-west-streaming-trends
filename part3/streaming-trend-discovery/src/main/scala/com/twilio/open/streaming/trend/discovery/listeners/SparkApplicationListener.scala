package com.twilio.open.streaming.trend.discovery.listeners

import kamon.Kamon
import kamon.metric.MetricsModule
import org.apache.spark._
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.slf4j.{Logger, LoggerFactory}

object SparkApplicationListener {
  val log: Logger = LoggerFactory.getLogger(classOf[SparkApplicationListener])

  def apply(spark: SparkSession): SparkApplicationListener = {
    new SparkApplicationListener(spark)
  }
}

class SparkApplicationListener(spark: SparkSession) extends SparkListener {
  import SparkApplicationListener._

  val defaultTags = Map(
    "app_name" -> spark.sparkContext.appName,
    "app_id" -> spark.sparkContext.applicationId
  )

  lazy val metricsModule: MetricsModule = {
    val metrics = Kamon.metrics
    Kamon.start() // if already started is noop
    metrics
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    log.info(s"onApplicationStart: $applicationStart")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info(s"onApplicationEnd: $applicationEnd")
    Kamon.shutdown()
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    log.info(s"onTaskGettingResult: $taskGettingResult")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    val stageId = taskEnd.stageId
    val stageAttemptId = taskEnd.stageAttemptId

    // taskInfo
    val taskInfo = taskEnd.taskInfo
    val executorId = taskInfo.executorId
    val taskId = taskInfo.taskId

    val  taskTags = defaultTags + ("executor_id" -> executorId)

    metricsModule.histogram("spark.task.duration", taskTags).record(taskInfo.duration)

    val taskMetrics = taskEnd.taskMetrics
    if (taskMetrics != null && taskMetrics.inputMetrics != null && taskMetrics.outputMetrics != null) {
      val recordsRead = taskMetrics.inputMetrics.recordsRead
      val bytesRead = taskMetrics.inputMetrics.bytesRead
      val recordsWritten = taskMetrics.outputMetrics.recordsWritten
      val bytesWritten = taskMetrics.outputMetrics.bytesWritten

      val executorCPUTime = taskMetrics.executorCpuTime
      metricsModule.histogram("spark.task.executor.cpu.time", taskTags).record(executorCPUTime)

      val jvmGCTime = taskMetrics.jvmGCTime
      metricsModule.histogram("spark.task.jvm.gc.time", taskTags).record(jvmGCTime)

      val executorRunTime = taskMetrics.executorRunTime
      metricsModule.histogram("spark.task.executor.run.time", taskTags).record(executorRunTime)

      // memory health
      val diskBytesSpilled = taskMetrics.diskBytesSpilled
      metricsModule.histogram("spark.task.disk.bytes.spilled", taskTags).record(diskBytesSpilled)

      val memoryBytesSpilled = taskMetrics.memoryBytesSpilled
      metricsModule.histogram("spark.task.memory.bytes.spilled", taskTags).record(memoryBytesSpilled)

      val peakExecutionMemory = taskMetrics.peakExecutionMemory
      metricsModule.histogram("spark.task.peak.execution.memory", taskTags).record(peakExecutionMemory)

      val resultSize = taskMetrics.resultSize
      metricsModule.histogram("spark.task.result.size", taskTags).record(resultSize)

      val executorDeserializationCPUTime = taskMetrics.executorDeserializeCpuTime
      val executorDeserializationTime = taskMetrics.executorDeserializeTime
      val resultSerializationTime = taskMetrics.resultSerializationTime

      if (log.isDebugEnabled) {
        log.debug(s"task.end stage.id=$stageId stage.attempt.id=$stageAttemptId records.read=$recordsRead " +
          s"bytes.read=$bytesRead records.written=$recordsWritten bytes.written=$bytesWritten " +
          s"disk.bytes.spilled=$diskBytesSpilled executor.cpu.time=$executorCPUTime " +
          s"executor.deserialization.cpu.time=$executorDeserializationCPUTime " +
          s"executor.deserialization.time=$executorDeserializationTime executor.run.time=$executorRunTime " +
          s"jvm.gc.time=$jvmGCTime result.size=$resultSize result.serialization.time=$resultSerializationTime " +
          s"peak.execution.memory=$peakExecutionMemory")
      }
    }

    // track task success / failure
    val taskOutcomeTracker = if (taskInfo.failed) "spark.task.failed" else "spark.task.completed"
    metricsModule.counter(taskOutcomeTracker, taskTags).increment()

    // todo - connect these warn/error to rollbar
    taskEnd.reason match {
      case Success =>
        log.debug(s"completed:Success taskId=$taskId taskType=${taskEnd.taskType}")
      case Resubmitted =>
        log.warn(s"completed:Failed taskId=$taskId reason=resubmitted")
      case TaskResultLost =>
        log.warn(s"completed:Failed taskId=$taskId reason=resultLost")
      case TaskKilled(reason) =>
        log.warn(s"completed:Failed taskId=$taskId reason=$reason")
      case UnknownReason =>
        log.warn(s"completed:Failed taskId=$taskId reason=unknown")
      case ExceptionFailure(className, _, _, fullStackTrace, _, _, _) =>
        log.warn(s"task.failed className=$className stackTrace=$fullStackTrace")
      case ExecutorLostFailure(execId, exitedByApp, _) =>
        log.warn(s"task.failed reason=lostExecutor executor.id=$execId app.caused.failure=$exitedByApp")
      case FetchFailed(_, _, _, _, message) =>
        log.warn(s"task.failed fetch.data.failure message=$message")
      case TaskCommitDenied(jobId, _, attemptNumber) =>
        log.warn(s"task.failed reason=taskCommitDenited jobId=$jobId attemptNumber=$attemptNumber")
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    metricsModule.minMaxCounter("spark.executors", defaultTags).increment()
    val executorId = executorAdded.executorId
    val addedAt = executorAdded.time
    val info = executorAdded.executorInfo
    val totalCores = info.totalCores
    val executorHost = info.executorHost
    log.info(s"added.executor id=$executorId addedTime=$addedAt totalCores=$totalCores executorHost=$executorHost")
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    metricsModule.minMaxCounter("spark.executors", defaultTags).decrement()
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlExecutionStart: SparkListenerSQLExecutionStart =>
        val description = sqlExecutionStart.description //
        log.debug(s"sql_execution_start time=${sqlExecutionStart.time} description=$description")
      case sqlExecutionEnd: SparkListenerSQLExecutionEnd =>
        metricsModule.histogram("spark.sql.execution.time", defaultTags).record(sqlExecutionEnd.time)
      case _ =>
        // spark query events (progress, termination) handled in monitorStreams method
        log.debug(s"unhandled event: $event")
    }
  }
}
