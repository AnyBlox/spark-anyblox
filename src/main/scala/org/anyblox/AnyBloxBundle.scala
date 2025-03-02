package org.anyblox

import java.io.FileDescriptor

import org.anyblox.ffi.AnyBloxNative
import org.anyblox.spark.AnyBloxBundleKey

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.c.Data
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

sealed trait AnyBloxBundle extends AutoCloseable {
  def asRawHandle: Long
  def metadata: AnyBloxMetadata
}

class AnyBloxBundleBase {
  private val allocator = new RootAllocator
  private var metadata: Option[AnyBloxMetadata] = None

  def getMetadata(handle: Long): AnyBloxMetadata = {
    metadata match {
      case Some(m) => m
      case None =>
        val raw_data = AnyBloxNative.bundleMetadataData(handle)
        val raw_decoder = AnyBloxNative.bundleMetadataDecoder(handle)
        val raw_schema = AnyBloxNative.bundleMetadataSchema(handle)

        val data = AnyBloxDataMetadata(
          raw_data(0).asInstanceOf[String],
          raw_data(1).asInstanceOf[Long],
          Option(raw_data(3).asInstanceOf[Long]),
          Option(raw_data(2).asInstanceOf[String]))
        val decoder = AnyBloxDecoderMetadata(
          raw_decoder(0).asInstanceOf[String],
          Option(raw_decoder(1).asInstanceOf[String]),
          Option(raw_decoder(2).asInstanceOf[String]),
          Option(raw_decoder(3).asInstanceOf[String]),
          Option(raw_decoder(4).asInstanceOf[Long]))
        val schema = Data.importSchema(allocator, ArrowSchema.wrap(raw_schema), null)

        val m = AnyBloxMetadata(data, decoder, schema)
        metadata = Some(m)
        m
    }
  }
}

case class SelfContained private (private val handle: Long)
    extends AnyBloxBundleBase
    with AnyBloxBundle {
  override def asRawHandle: Long = handle
  override def metadata: AnyBloxMetadata = getMetadata(handle)

  override def close(): Unit = AnyBloxNative.dropBundle(handle)
}

case class Extension private (private val handle: Long)
    extends AnyBloxBundleBase
    with AnyBloxBundle {
  override def asRawHandle: Long = handle
  override def metadata: AnyBloxMetadata = getMetadata(handle)

  override def close(): Unit = AnyBloxNative.dropBundle(handle)
}

object AnyBloxBundle {
  def openExtensionAndData(anybloxPath: String, dataPath: String): AnyBloxBundle = {
    using(TransparentFile.openFile(anybloxPath)) { anybloxFile =>
      using(TransparentFile.openFile(dataPath)) { dataFile =>
        openExtensionAndData(anybloxFile.asInt, anybloxFile.len, dataFile.asInt, dataFile.len)
      }
    }
  }
  def openExtensionAndData(
      anybloxFd: FileDescriptor,
      anybloxLen: Long,
      dataPath: String): AnyBloxBundle = {
    using(TransparentFile.openFile(dataPath)) { dataFile =>
      openExtensionAndData(
        TransparentFile.getRawFd(anybloxFd),
        anybloxLen,
        dataFile.asInt,
        dataFile.len)
    }
  }
  def openExtensionAndData(
      anybloxPath: String,
      dataFd: FileDescriptor,
      dataLen: Long): AnyBloxBundle = {
    using(TransparentFile.openFile(anybloxPath)) { anybloxFile =>
      openExtensionAndData(
        anybloxFile.asInt,
        anybloxFile.len,
        TransparentFile.getRawFd(dataFd),
        dataLen)
    }
  }
  def openExtensionAndData(
      anybloxFd: FileDescriptor,
      anybloxLen: Long,
      dataFd: FileDescriptor,
      dataLen: Long): AnyBloxBundle = {
    openExtensionAndData(
      TransparentFile.getRawFd(anybloxFd),
      anybloxLen,
      TransparentFile.getRawFd(dataFd),
      dataLen)
  }

  private def openExtensionAndData(
      anybloxFd: Int,
      anybloxLen: Long,
      dataFd: Int,
      dataLen: Long): AnyBloxBundle = {
    val handle =
      AnyBloxNative.bundleOpenExtensionAndData(anybloxFd, anybloxLen, dataFd, dataLen)
    Extension(handle)
  }

  def openSelfContained(path: String): AnyBloxBundle = {
    using(TransparentFile.openFile(path)) { file =>
      openSelfContained(file.asInt, file.len)
    }
  }

  def openSelfContained(fd: FileDescriptor, len: Long): AnyBloxBundle = {
    openSelfContained(TransparentFile.getRawFd(fd), len)
  }

  private def openSelfContained(fd: Int, len: Long): AnyBloxBundle = {
    val handle = AnyBloxNative.bundleOpenSelfContained(fd, len)
    SelfContained(handle)
  }

  def using[A <: AutoCloseable, B](resource: A)(block: A => B): B =
    try block(resource)
    finally resource.close()
}

private class TransparentFile private (
    val fd: FileDescriptor,
    val len: Long,
    private val handle: AutoCloseable)
    extends AutoCloseable {
  def asInt: Int = TransparentFile.getRawFd(fd)

  override def close(): Unit = {
    handle.close()
  }
}

private object TransparentFile {
  def openFile(path: String): TransparentFile = {
    val file = new java.io.File(path)
    val len = file.length
    val is = new java.io.FileInputStream(file)
    new TransparentFile(is.getFD, len, is)
  }

  def getRawFd(fd: FileDescriptor): Int = {
    val field = fd.getClass.getDeclaredField("fd")
    field.setAccessible(true)
    val v = field.getInt(fd)
    assert(v > 0)
    v
  }
}
