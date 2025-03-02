package org.anyblox.spark

import org.apache.arrow.vector.types.{FloatingPointPrecision, IntervalUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, FieldType}
import org.apache.spark.sql.types._

object ArrowUtil {
  def translateType(fieldType: FieldType): DataType = translateType(fieldType.getType)

  def translateType(arrowType: ArrowType): DataType = arrowType.accept(TypeVisitor())

  private case class TypeVisitor() extends ArrowType.ArrowTypeVisitor[DataType] {
    override def visit(`type`: ArrowType.Null): DataType = NullType

    override def visit(`type`: ArrowType.Struct): DataType = ???

    override def visit(`type`: ArrowType.List): DataType = ???

    override def visit(`type`: ArrowType.LargeList): DataType = ???

    override def visit(`type`: ArrowType.FixedSizeList): DataType = ???

    override def visit(`type`: ArrowType.Union): DataType = ???

    override def visit(`type`: ArrowType.Map): DataType = ???

    override def visit(`type`: ArrowType.Int): DataType = `type`.getBitWidth match {
      case 8 => ByteType
      case 16 => ShortType
      case 32 => IntegerType
      case 64 => LongType
    }

    override def visit(`type`: ArrowType.FloatingPoint): DataType = `type`.getPrecision match {
      case FloatingPointPrecision.HALF => FloatType
      case FloatingPointPrecision.SINGLE => FloatType
      case FloatingPointPrecision.DOUBLE => DoubleType
    }

    override def visit(`type`: ArrowType.Utf8): DataType = StringType

    override def visit(`type`: ArrowType.LargeUtf8): DataType = StringType

    override def visit(`type`: ArrowType.Binary): DataType = BinaryType

    override def visit(`type`: ArrowType.LargeBinary): DataType = BinaryType

    override def visit(`type`: ArrowType.FixedSizeBinary): DataType =
      if (`type`.getByteWidth == 1) { CharType(1) }
      else { BinaryType }

    override def visit(`type`: ArrowType.Bool): DataType = BooleanType

    override def visit(`type`: ArrowType.Decimal): DataType =
      DecimalType(`type`.getPrecision, `type`.getScale)

    override def visit(`type`: ArrowType.Date): DataType = DateType

    override def visit(`type`: ArrowType.Time): DataType = ???

    override def visit(`type`: ArrowType.Timestamp): DataType = TimestampType

    override def visit(`type`: ArrowType.Interval): DataType = `type`.getUnit match {
      case IntervalUnit.YEAR_MONTH => YearMonthIntervalType.DEFAULT
      case IntervalUnit.DAY_TIME => DayTimeIntervalType.DEFAULT
      case IntervalUnit.MONTH_DAY_NANO => CalendarIntervalType
    }

    override def visit(`type`: ArrowType.Duration): DataType = ???

    override def visit(`type`: ArrowType.ListView): DataType = ???

    override def visit(`type`: ArrowType.BinaryView): DataType = BinaryType

    override def visit(`type`: ArrowType.Utf8View): DataType = StringType
  }
}
