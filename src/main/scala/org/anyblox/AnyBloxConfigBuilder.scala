package org.anyblox

import org.anyblox.LogLevel._
import org.anyblox.ffi.AnyBloxNative

class AnyBloxConfigBuilder extends AutoCloseable {
  private val handle = AnyBloxNative.createConfigBuilder();
  private var finished = false

  def setWasmCacheLimit(limit: Long): AnyBloxConfigBuilder = {
    assertActive()
    AnyBloxNative.configBuilderSetWasmCacheLimit(handle, limit)
    this
  }

  def setThreadVirtualMemoryLimit(limit: Long): AnyBloxConfigBuilder = {
    assertActive()
    AnyBloxNative.configBuilderSetThreadVirtualMemoryLimit(handle, limit)
    this
  }

  def setLogLevel(logLevel: LogLevel): AnyBloxConfigBuilder = {
    assertActive();
    AnyBloxNative.configBuilderSetLogLevel(handle, logLevel.asInt)
    this
  }

  def setLogDirectory(path: String): AnyBloxConfigBuilder = {
    assertActive()
    AnyBloxNative.configBuilderSetLogDirectory(handle, path)
    this
  }

  def compileWithDebug(value: Boolean): AnyBloxConfigBuilder = {
    assertActive()
    AnyBloxNative.configBuilderCompileWithDebug(handle, value)
    this
  }

  def build(): AnyBloxConfig = {
    assertActive()
    val configHandle = AnyBloxNative.configBuilderFinish(handle)
    finished = true
    new AnyBloxConfig(configHandle)
  }

  private def assertActive(): Unit = {
    if (finished) {
      throw new AnyBloxRuntimeException("Usage of consumed AnyBloxConfigBuilder is not allowed")
    }
  }

  override def close(): Unit = {
    if (!finished) {
      AnyBloxNative.dropConfigBuilder(handle)
    }
  }
}

object AnyBloxConfigBuilder {
  def default: AnyBloxConfigBuilder = {
    new AnyBloxConfigBuilder()
      .setWasmCacheLimit(64 * 1024 * 1024)
      .setLogLevel(Info)
  }
}
