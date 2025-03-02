package org.anyblox.spark

import scala.collection.JavaConverters._

import org.anyblox.{AnyBloxBundle, AnyBloxJobParametersBuilder, AnyBloxRuntime, ColumnProjection}
import org.anyblox.spark.metrics.AnyBloxTaskMetrics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class AnyBloxReader private (
    bundle: AnyBloxBundle,
    firstTuple: Long,
    tupleCount: Long,
    batchSize: Long,
    projection: ColumnProjection,
    schema: StructType)
    extends PartitionReader[ColumnarBatch]
    with Logging
    with AutoCloseable {
  private val metrics = AnyBloxTaskMetrics()
  private val job = {
    val jobParams =
      new AnyBloxJobParametersBuilder().with_column_projection(projection).finish(bundle)
    val job = metrics.initTimeMetric.time { () =>
      AnyBloxRuntime.get.initBlockingJob(jobParams)
    }
    logInfo(s"Job schema: ${job.field}")
    job
  }

  private val fieldVector = job.createVector
  private val batch = new ColumnarBatch(
    fieldVector.getChildrenFromFields.asScala
      .zip(schema.fields)
      .zip(job.field.getChildren.asScala)
      .map { x =>
        AnyBloxVector(x._2, x._1._1, x._1._2.dataType)
      }
      .toArray)
  private var startTuple = firstTuple
  private val tupleLimit = startTuple + tupleCount

  override def next(): Boolean = {
    if (tupleLimit <= startTuple) {
      batch.setNumRows(0)
      return false
    }
    val request = (tupleLimit - startTuple) min batchSize
    logInfo("Next batch request: " + request)
    val ts = System.nanoTime()
    val decodeDuration = AnyBloxRuntime.get.decodeBatch(job, startTuple, request, fieldVector)
    if (fieldVector.getValueCount == 0) {
      logInfo("Empty?")
      batch.setNumRows(0)
      return false
    }

    logInfo("Received " + fieldVector.getValueCount)
    logInfo("fieldVector schema: " + fieldVector.getField)
    logInfo("fieldVector #children: " + fieldVector.getChildrenFromFields.size)
    batch.setNumRows(fieldVector.getValueCount)
    startTuple += fieldVector.getValueCount

    val duration = System.nanoTime() - ts
    metrics.decodeBatchTimeMetric.add(decodeDuration)
    metrics.batchLoadTimeMetric.add(duration - decodeDuration)
    true
  }

  override def get(): ColumnarBatch = batch

  override def close(): Unit = job.close()

  override def currentMetricsValues(): Array[CustomTaskMetric] = metrics.asArray
}

object AnyBloxReader {
  def create(
      partition: AnyBloxInputPartition,
      bundle: AnyBloxBundle,
      projection: ColumnProjection,
      schema: StructType): AnyBloxReader = {
    new AnyBloxReader(
      bundle,
      partition.firstTuple,
      partition.tupleCount,
      partition.batchSize,
      projection,
      schema)
  }
}
