package org.anyblox

import org.anyblox.ffi.AnyBloxNative

class AnyBloxConfig private[anyblox] (private val handle: Long) extends AutoCloseable {
  private var consumed = false

  def consume(): Long = {
    if (consumed) {
      throw new AnyBloxRuntimeException("Usage of consumed AnyBloxConfig is not allowed")
    }
    consumed = true
    handle
  }

  override def close(): Unit = {
    if (!consumed) {
      AnyBloxNative.dropConfig(handle)
    }
  }
}
