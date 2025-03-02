package org.anyblox.spark

import org.anyblox.{ColumnProjection, AnyBloxBundle}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

class AnyBloxScanBuilder(
    sparkSession: SparkSession,
    key: AnyBloxBundleKey,
    bundle: AnyBloxBundle,
    private var schema: StructType,
    batchSize: Long,
    numThreads: Option[Int])
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with Logging {
  private var columnProjection: ColumnProjection = ColumnProjection.allColumns(schema.length)

  override def build(): Scan =
    new AnyBloxScan(sparkSession, key, bundle, columnProjection, schema, batchSize, numThreads)

  override def pruneColumns(requiredSchema: StructType): Unit = {
    columnProjection = ColumnProjection.empty
    for (requiredField <- requiredSchema) {
      val idx = schema.fieldIndex(requiredField.name)
      columnProjection = columnProjection.add(idx)
    }
    schema = requiredSchema
    logInfo(s"Pruned schema: $schema, projection: $columnProjection")
  }
}
