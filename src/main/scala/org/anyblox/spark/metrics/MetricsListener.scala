package org.anyblox.spark.metrics

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui._

import upickle.default._

case class ExecutedPlanNode(
    nodeName: String,
    description: String,
    metadata: Map[String, String],
    metrics: Seq[MetricValue],
    children: Seq[ExecutedPlanNode]) {}

case class MetricValue(metricName: String, value: Long)
object MetricValue {
  implicit val metricValueRw: ReadWriter[MetricValue] = macroRW
}

class MetricInfo(val metricInfo: SQLMetricInfo, val value: Long)

object ExecutedPlanNode {
  implicit val executedPlanNodeRw: ReadWriter[ExecutedPlanNode] = macroRW

  def build(rawPlan: SparkPlanInfo, metrics: Map[Long, MetricInfo]): ExecutedPlanNode = {
    new ExecutedPlanNode(
      rawPlan.nodeName,
      rawPlan.simpleString,
      rawPlan.metadata,
      rawPlan.metrics.map { m => MetricValue(m.name, metrics(m.accumulatorId).value) },
      rawPlan.children.map { c => ExecutedPlanNode.build(c, metrics) })
  }
}

case class ExecutedPlan(totalTime: Long, plan: ExecutedPlanNode) {
  def toJson: String = {
    upickle.default.write(this)
  }
}
object ExecutedPlan {
  implicit val executedPlanRw: ReadWriter[ExecutedPlan] = macroRW
}

class ExecutionReport(
    val totalTime: Long,
    val metrics: Map[Long, MetricInfo],
    val plan: SparkPlanInfo) {
  def printablePlan: ExecutedPlan = {
    ExecutedPlan(totalTime, ExecutedPlanNode.build(plan, metrics))
  }

  def toJson: String = {
    upickle.default.write(printablePlan)
  }
}

class SQLExecutionAnalysis(val startEvent: SparkListenerJobStart) {
  private var sqlStartEvent: Option[SparkListenerSQLExecutionStart] = None
  private var endEvent: Option[SparkListenerSQLExecutionEnd] = None
  private val adaptiveExecutionUpdates: ArrayBuffer[SparkListenerSQLAdaptiveExecutionUpdate] =
    ArrayBuffer()
  private val adaptiveMetricUpdates: ArrayBuffer[SparkListenerSQLAdaptiveSQLMetricUpdates] =
    ArrayBuffer()
  private val accumulatorUpdates: ArrayBuffer[(Long, Long)] = ArrayBuffer()

  def onAdaptiveExecutionUpdate(e: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    adaptiveExecutionUpdates.append(e)
  }

  def onAdaptiveMetricUpdate(e: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    adaptiveMetricUpdates.append(e)
  }

  def onDriverAccumUpdate(e: SparkListenerDriverAccumUpdates): Unit = {
    accumulatorUpdates.appendAll(e.accumUpdates)
  }

  def onStart(e: SparkListenerSQLExecutionStart): Unit = {
    sqlStartEvent = Some(e)
  }

  def onEnd(e: SparkListenerSQLExecutionEnd): Unit = {
    endEvent = Some(e)
  }

  def onExecutorMetricsUpdate(event: Seq[AccumulableInfo]): Unit = {
    accumulatorUpdates ++= event.map { accumulableToAccumUpdate }
  }

  def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    accumulatorUpdates ++= event.taskInfo.accumulables.map { accumulableToAccumUpdate }
  }

  def report: ExecutionReport = {
    assert(endEvent.isDefined)
    val aggregatedAccums =
      accumulatorUpdates.groupBy(m => m._1).mapValues { s => s.map { x => x._2 }.sum }
    val metrics = allMetrics.map { case (id, m) =>
      (id, new MetricInfo(m, aggregatedAccums.getOrElse(id, 0)))
    }

    new ExecutionReport(endEvent.get.time - sqlStartEvent.get.time, metrics, plan)
  }

  private def accumulableToAccumUpdate(acc: AccumulableInfo): (Long, Long) = {
    val value = acc.update.get match {
      case s: String => s.toLong
      case l: Long => l
      case o => throw SparkException.internalError(s"Unexpected accumulator value: $o")
    }

    (acc.id, value)
  }

  private def plan: SparkPlanInfo = {
    adaptiveExecutionUpdates
      .map { u => u.sparkPlanInfo }
      .lastOption
      .getOrElse(sqlStartEvent.get.sparkPlanInfo)
  }

  private def allMetrics: Map[Long, SQLMetricInfo] = {
    (gatherMetricsInPlan(sqlStartEvent.get.sparkPlanInfo) ++ adaptiveExecutionUpdates.flatMap {
      e =>
        gatherMetricsInPlan(e.sparkPlanInfo)
    } ++ adaptiveMetricUpdates.flatMap { e => e.sqlPlanMetrics }.map { m =>
      new SQLMetricInfo(m.name, m.accumulatorId, m.metricType)
    }).map { m =>
      (m.accumulatorId, m)
    }.toMap
  }

  private def gatherMetricsInPlan(plan: SparkPlanInfo): Iterable[SQLMetricInfo] = {
    plan.metrics ++ plan.children.flatMap { c => gatherMetricsInPlan(c) }
  }
}

class AggregateExecutionReport(val execs: Seq[ExecutionReport]) {
  def toJson: String = {
    upickle.default.write(execs.map { e => e.printablePlan })
  }
}

class ExecutionEventsProcessor {
  private var execs: Map[Long, SQLExecutionAnalysis] = Map()
  private var stageToExec: Map[Long, Long] = Map()

  def processJobStart(event: SparkListenerJobStart): Unit = {
    val executionIdString = event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }

    val executionId = executionIdString.toLong
    execs = execs + (executionId -> new SQLExecutionAnalysis(event))
    stageToExec ++= event.stageInfos.map { s => s.stageId.toLong -> executionId }
  }

  def processSqlExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    execs(event.executionId).onStart(event)
  }

  def processSqlAdaptiveExecutionUpdate(event: SparkListenerSQLAdaptiveExecutionUpdate): Unit = {
    if (execs.contains(event.executionId)) {
      execs(event.executionId).onAdaptiveExecutionUpdate(event)
    }
  }

  def processSqlAdaptiveMetricUpdate(event: SparkListenerSQLAdaptiveSQLMetricUpdates): Unit = {
    if (execs.contains(event.executionId)) {
      execs(event.executionId).onAdaptiveMetricUpdate(event)
    }
  }

  def processSqlDriverAccumUpdate(event: SparkListenerDriverAccumUpdates): Unit = {
    if (execs.contains(event.executionId)) {
      execs(event.executionId).onDriverAccumUpdate(event)
    }
  }

  def processSqlExecutionEnd(event: SparkListenerSQLExecutionEnd): Unit = {
    if (execs.contains(event.executionId)) {
      execs(event.executionId).onEnd(event)
    }
  }

  def processExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    event.accumUpdates.foreach { case (_, stageId, _, accumUpdates) =>
      if (stageToExec.contains(stageId)) {
        execs(stageToExec(stageId)).onExecutorMetricsUpdate(accumUpdates)
      }
    }
  }

  def processTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (stageToExec.contains(event.stageId)) {
      val executionId = stageToExec(event.stageId)
      execs(executionId).onTaskEnd(event)
    }
  }

  def report: AggregateExecutionReport = {
    new AggregateExecutionReport(execs.map { e => e._2.report }.toSeq)
  }
}

class ExecutionEventListener extends org.apache.spark.scheduler.SparkListener {
  private val jobStarts: ArrayBuffer[SparkListenerJobStart] = ArrayBuffer()
  private val executorMetricsUpdates: ArrayBuffer[SparkListenerExecutorMetricsUpdate] =
    ArrayBuffer()
  private val taskEnds: ArrayBuffer[SparkListenerTaskEnd] = ArrayBuffer()
  private val sqlStarts: ArrayBuffer[SparkListenerSQLExecutionStart] = ArrayBuffer()
  private val sqlAdaptiveExecutionUpdates: ArrayBuffer[SparkListenerSQLAdaptiveExecutionUpdate] =
    ArrayBuffer()
  private val sqlAdaptiveMetricsUpdates: ArrayBuffer[SparkListenerSQLAdaptiveSQLMetricUpdates] =
    ArrayBuffer()
  private val sqlDriverAccumUpdates: ArrayBuffer[SparkListenerDriverAccumUpdates] = ArrayBuffer()
  private val sqlEnds: ArrayBuffer[SparkListenerSQLExecutionEnd] = ArrayBuffer()

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    jobStarts.append(event)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart => sqlStarts.append(e)
      case e: SparkListenerSQLExecutionEnd => sqlEnds.append(e)
      case e: SparkListenerSQLAdaptiveExecutionUpdate => sqlAdaptiveExecutionUpdates.append(e)
      case e: SparkListenerSQLAdaptiveSQLMetricUpdates => sqlAdaptiveMetricsUpdates.append(e)
      case e: SparkListenerDriverAccumUpdates => sqlDriverAccumUpdates.append(e)
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    executorMetricsUpdates.append(event)
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    taskEnds.append(event)
  }

  def report: AggregateExecutionReport = {
    Thread.`yield`()
    val processor = new ExecutionEventsProcessor

    jobStarts.foreach { processor.processJobStart }
    sqlStarts.foreach { processor.processSqlExecutionStart }
    sqlAdaptiveExecutionUpdates.foreach { processor.processSqlAdaptiveExecutionUpdate }
    sqlAdaptiveMetricsUpdates.foreach { processor.processSqlAdaptiveMetricUpdate }
    sqlDriverAccumUpdates.foreach { processor.processSqlDriverAccumUpdate }
    executorMetricsUpdates.foreach { processor.processExecutorMetricsUpdate }
    taskEnds.foreach { processor.processTaskEnd }
    sqlEnds.foreach { processor.processSqlExecutionEnd }

    processor.report
  }

  def clear(): Unit = {
    Thread.`yield`()
    jobStarts.clear()
    sqlStarts.clear()
    sqlAdaptiveExecutionUpdates.clear()
    sqlAdaptiveMetricsUpdates.clear()
    sqlDriverAccumUpdates.clear()
    executorMetricsUpdates.clear()
    taskEnds.clear()
    sqlEnds.clear()
  }
}

object ExecutionEventListener {
  def create(sc: SparkContext): ExecutionEventListener = {
    val listener = new ExecutionEventListener
    sc.addSparkListener(listener)
    listener
  }
}
