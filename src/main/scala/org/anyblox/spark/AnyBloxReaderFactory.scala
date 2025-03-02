package org.anyblox.spark

import org.anyblox.{ColumnProjection, AnyBloxBundle}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class AnyBloxReaderFactory(
    bundle: AnyBloxBundle,
    projection: ColumnProjection,
    schema: StructType)
    extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    throw new UnsupportedOperationException("Only 'createColumnarReader' is supported")

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    val p = partition.asInstanceOf[AnyBloxInputPartition]
    AnyBloxReader.create(p, bundle, projection, schema)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true
}
