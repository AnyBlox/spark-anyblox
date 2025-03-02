package org.anyblox.spark.metrics

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}

object AnyBloxMetrics {
  final val AnyBloxInitTimeName: String = "AnyBloxInitTime"
  final val AnyBloxDecodeBatchTimeName: String = "AnyBloxDecodeBatchTime"
  final val AnyBloxBatchLoadTimeName: String = "AnyBloxBatchLoadTime"

  def customMetrics: Array[CustomMetric] = {
    Array(
      AnyBloxInitTimeMetric(),
      AnyBloxDecodeBatchTimeMetric(),
      AnyBloxBatchLoadTimeMetric())
  }
}

class CounterMetric(val name: String, val description: String) extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = s"${taskMetrics.sum}"
}

class NanoTimeMetric(val name: String, val description: String) extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String =
    f"${taskMetrics.sum / 1000000.0}%.3fms"
}

case class AnyBloxInitTimeMetric()
    extends NanoTimeMetric(AnyBloxMetrics.AnyBloxInitTimeName, "time spent in job init")
case class AnyBloxDecodeBatchTimeMetric()
    extends NanoTimeMetric(
      AnyBloxMetrics.AnyBloxDecodeBatchTimeName,
      "time spent in decode_batch")
case class AnyBloxBatchLoadTimeMetric()
    extends NanoTimeMetric(
      AnyBloxMetrics.AnyBloxBatchLoadTimeName,
      "time spent loading native vectors")

class CounterTaskMetric(val name: String) extends CustomTaskMetric {
  private var v: Long = 0

  def add(value: Long): Unit = v += value
  override def value(): Long = v
}

class NanoTimeTaskMetric(val name: String) extends CustomTaskMetric {
  private var ns: Long = 0

  def add(value: Long): Unit = ns += value
  override def value(): Long = ns

  def time[T](f: () => T): T = {
    val ts = System.nanoTime()
    val res = f()
    val total = System.nanoTime() - ts
    ns += total
    res
  }
}

case class AnyBloxInitTimeTaskMetric()
    extends NanoTimeTaskMetric(AnyBloxMetrics.AnyBloxInitTimeName)
case class AnyBloxDecodeBatchTimeTaskMetric()
    extends NanoTimeTaskMetric(AnyBloxMetrics.AnyBloxDecodeBatchTimeName)
case class AnyBloxBatchLoadTimeTaskMetric()
    extends NanoTimeTaskMetric(AnyBloxMetrics.AnyBloxBatchLoadTimeName)

case class AnyBloxTaskMetrics() {
  val initTimeMetric: NanoTimeTaskMetric = AnyBloxInitTimeTaskMetric()
  val decodeBatchTimeMetric: NanoTimeTaskMetric = AnyBloxDecodeBatchTimeTaskMetric()
  val batchLoadTimeMetric: NanoTimeTaskMetric = AnyBloxBatchLoadTimeTaskMetric()

  def asArray: Array[CustomTaskMetric] =
    Array(initTimeMetric, decodeBatchTimeMetric, batchLoadTimeMetric)
}
