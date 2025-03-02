package org.anyblox.spark
import java.util

import scala.collection.JavaConverters._

import org.anyblox.{AnyBloxBundle, AnyBloxMetadata}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Column, SupportsRead, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class AnyBloxTable private[anyblox] (
    val sparkSession: SparkSession,
    val key: AnyBloxBundleKey,
    val bundle: AnyBloxBundle)
    extends SupportsRead
    with Logging
    with AutoCloseable {
  private lazy val capabilitiesSet: util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val batchSize =
      options.getLong("batchSize", 1) max bundle.metadata.decoder.min_batch_size.getOrElse(1)
    val rawNumThreads = options.getInt("threads", 0)
    val numThreads = if (rawNumThreads > 0) {
      Some(rawNumThreads)
    } else {
      None
    }

    new AnyBloxScanBuilder(sparkSession, key, bundle, schema(), batchSize, numThreads)
  }

  override def name(): String = key.idString

  override def capabilities(): util.Set[TableCapability] = {
    capabilitiesSet
  }

  override def columns(): Array[Column] = {
    val schema = bundle.metadata.schema
    schema.getFields.asScala.map { field =>
      Column.create(field.getName, ArrowUtil.translateType(field.getFieldType), field.isNullable)
    }.toArray
  }

  override def schema(): StructType = {
    val schema = bundle.metadata.schema
    val fields = schema.getFields.asScala.map { field =>
      StructField(
        field.getName,
        ArrowUtil.translateType(field.getFieldType),
        field.isNullable,
        null)
    }.toArray

    logInfo("anyblox inferred schema: " + StructType(fields))

    new StructType(fields)
  }

  override def close(): Unit = bundle.close()
}

object AnyBloxTable {
  def takeBundle(
      sparkSession: SparkSession,
      key: AnyBloxBundleKey,
      bundle: AnyBloxBundle): AnyBloxTable = {
    new AnyBloxTable(sparkSession, key, bundle)
  }
}
