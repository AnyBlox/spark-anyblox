package org.anyblox.spark

import java.util.OptionalLong

import org.anyblox.{AnyBloxBundle, ColumnProjection}
import org.anyblox.spark.metrics.AnyBloxMetrics

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.internal.connector.SupportsMetadata
import org.apache.spark.sql.types.StructType

case class AnyBloxInputPartition(val firstTuple: Long, val tupleCount: Long, val batchSize: Long)
    extends InputPartition {}

class AnyBloxScan(
    sparkSession: SparkSession,
    key: AnyBloxBundleKey,
    bundle: AnyBloxBundle,
    projection: ColumnProjection,
    schema: StructType,
    batchSize: Long,
    numThreads: Option[Int])
    extends Scan
    with Batch
    with SupportsReportStatistics
    with SupportsMetadata
    with Logging {
  private val targetNumPartitions = numThreads.getOrElse {
    sparkSession.sparkContext.defaultParallelism
  }.longValue max 1L
  private final val minTuplesPerPartition =
    1024L max bundle.metadata.decoder.min_batch_size.getOrElse(0L)

  override def readSchema(): StructType = schema
  override def planInputPartitions(): Array[InputPartition] = {
    // tpp = rowCount / numPartitions
    // tpp >= minTuplesPerPartition
    // rowCount / numPartitions >= minTuplesPerPartition
    // rowCount / minTuplesPerPartition >= numPartitions
    val rowCount = bundle.metadata.data.count
    val minNumPartitions = (rowCount / minTuplesPerPartition) max 1L
    val numPartitions = targetNumPartitions min minNumPartitions

    val tuplesPerPartition = bundle.metadata.data.count / numPartitions
    logInfo(s"tuplesPerPartition = $tuplesPerPartition, numPartition = $numPartitions")

    (0L until numPartitions).map { part =>
      AnyBloxInputPartition(tuplesPerPartition * part, tuplesPerPartition, batchSize)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new AnyBloxReaderFactory(bundle, projection, schema)

  override def estimateStatistics(): Statistics = {
    val length = bundle.metadata.data.count
    val size = bundle.metadata.data.sizeInBytes

    new Statistics {
      override def sizeInBytes(): OptionalLong = size match {
        case Some(s) => OptionalLong.of(s)
        case None => OptionalLong.empty()
      }

      override def numRows(): OptionalLong = OptionalLong.of(length)
    }
  }

  override def toBatch: Batch = this

  override def getMetaData(): Map[String, String] = Map.empty

  override def supportedCustomMetrics(): Array[CustomMetric] =
    AnyBloxMetrics.customMetrics
}
