package org.anyblox

case class ColumnProjection(val mask: Long) {
  def add(i: Int): ColumnProjection = {
    if (i >= 64) {
      throw new IllegalArgumentException("max index of a column in a ColumnProjection is 64")
    }
    ColumnProjection(mask | (1 << i))
  }

  override def toString: String = {
    mask.toBinaryString
  }
}

object ColumnProjection {
  def empty: ColumnProjection = ColumnProjection(0)

  def allColumns(n: Int): ColumnProjection =
    if (n == 64) {
      ColumnProjection(0xffffffffffffffffL)
    } else if (n > 64) {
      throw new IllegalArgumentException("max number of columns in a ColumnProjection is 64")
    } else {
      val x = 1L << n
      ColumnProjection(x - 1)
    }
}
