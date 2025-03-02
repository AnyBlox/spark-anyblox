package org.anyblox.spark

import org.apache.arrow.vector.{BaseVariableWidthViewVector, FieldVector}
import org.apache.arrow.vector.types.pojo.{Field => ArrowField}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarMap, ColumnVector}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

case class AnyBloxVector(arrowType: ArrowField, vector: FieldVector, vectorType: DataType)
    extends ColumnVector(vectorType) {

  override def hasNull: Boolean = vector.getNullCount > 0

  override def numNulls(): Int = vector.getNullCount

  override def isNullAt(rowId: Int): Boolean = vector.isNull(rowId)

  override def getBoolean(rowId: Int): Boolean = ???

  override def getByte(rowId: Int): Byte =
    Platform.getByte(null, vector.getDataBuffer.memoryAddress + rowId)

  override def getShort(rowId: Int): Short =
    Platform.getShort(null, vector.getDataBuffer.memoryAddress + rowId * 2L)

  override def getInt(rowId: Int): Int =
    Platform.getInt(null, vector.getDataBuffer.memoryAddress + rowId * 4L)

  override def getLong(rowId: Int): Long =
    Platform.getLong(null, vector.getDataBuffer.memoryAddress + rowId * 8L)

  override def getFloat(rowId: Int): Float =
    Platform.getFloat(null, vector.getDataBuffer.memoryAddress + rowId * 4L)

  override def getDouble(rowId: Int): Double =
    Platform.getDouble(null, vector.getDataBuffer.memoryAddress + rowId * 8L)

  override def getArray(rowId: Int): ColumnarArray = ???

  override def getMap(ordinal: Int): ColumnarMap = ???

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = ???

  override def getUTF8String(rowId: Int): UTF8String = {
    arrowType.getType match {
      case x: ArrowType.FixedSizeBinary => {
        val n = x.getByteWidth
        UTF8String.fromAddress(null, vector.getDataBuffer.memoryAddress + rowId * n, n)
      }
      case _: ArrowType.Utf8 | _: ArrowType.LargeUtf8 => {
        val offsetBufferAddress = vector.getOffsetBuffer.memoryAddress
        val offset = Platform.getInt(null, offsetBufferAddress + rowId * 4L)
        val length = Platform.getInt(null, offsetBufferAddress + (rowId + 1L) * 4L) - offset
        UTF8String.fromAddress(null, vector.getDataBuffer.memoryAddress + offset, length)
      }
      case _: ArrowType.Utf8View => {
        val viewVector = vector.asInstanceOf[BaseVariableWidthViewVector]
        val viewsBufferAddress = viewVector.getDataBuffer.memoryAddress
        val length = Platform.getInt(null, viewsBufferAddress + rowId * 16L)

        if (length <= BaseVariableWidthViewVector.INLINE_SIZE) {
          UTF8String.fromAddress(null, viewsBufferAddress + rowId * 16L + 4L, length)
        } else {
          val bufIdx = Platform.getInt(null, viewsBufferAddress + rowId * 16L + 8L)
          val offset = Platform.getInt(null, viewsBufferAddress + rowId * 16L + 12L)
          val buf = viewVector.getDataBuffers.get(bufIdx)
          UTF8String.fromAddress(null, buf.memoryAddress + offset, length)
        }
      }
    }
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    val bytes = new Array[Byte](1)
    val offset = vector.getDataBuffer.memoryAddress + rowId
    Platform.copyMemory(null, offset, bytes, Platform.BYTE_ARRAY_OFFSET, 1)
    bytes
  }

  override def getChild(ordinal: Int): ColumnVector = AnyBloxVector(
    arrowType.getChildren.get(ordinal),
    vector.getChildrenFromFields.get(ordinal),
    `type`.asInstanceOf[StructType].fields(ordinal).dataType)

  override def close(): Unit = vector.close()
}
