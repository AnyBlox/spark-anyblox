package org.anyblox
import org.anyblox.ffi.AnyBloxNative

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.types.pojo.Field

class AnyBloxJob private[anyblox] (
    private[anyblox] val handle: Long,
    private[anyblox] val field: Field,
    private val allocator: BufferAllocator)
    extends AutoCloseable {
  def createVector: FieldVector = field.createVector(allocator)

  override def close(): Unit = AnyBloxNative.dropJob(handle)
}

case class AnyBloxJobParameters private (
    columnProjection: Option[ColumnProjection],
    validate_utf8: Boolean,
    bundle: AnyBloxBundle)

class AnyBloxJobParametersBuilder() {
  private var validate_utf8 = true
  private var column_projection: Option[ColumnProjection] = None

  def do_not_validate_utf8(): AnyBloxJobParametersBuilder = {
    validate_utf8 = false
    this
  }

  def with_column_projection(projection: ColumnProjection): AnyBloxJobParametersBuilder = {
    column_projection = Some(projection)
    this
  }

  def finish(bundle: AnyBloxBundle): AnyBloxJobParameters = {
    AnyBloxJobParameters(column_projection, validate_utf8, bundle)
  }
}

class AnyBloxRuntime private (private val handle: Long) extends AutoCloseable {
  private val allocator = new RootAllocator

  private def unpackJobPointer(pointers: Array[Long]): AnyBloxJob = {
    val job_handle = pointers(0)
    val raw_schema = pointers(1)

    val field = Data.importField(allocator, ArrowSchema.wrap(raw_schema), null)
    new AnyBloxJob(job_handle, field, allocator)
  }

  def initBlockingJob(params: AnyBloxJobParameters): AnyBloxJob = {
    val pointers = params.columnProjection match {
      case Some(projection) =>
        AnyBloxNative.runtimeDecodeInitWithProjection(
          handle,
          params.bundle.asRawHandle,
          projection.mask,
          params.validate_utf8)
      case None =>
        AnyBloxNative.runtimeDecodeInit(handle, params.bundle.asRawHandle, params.validate_utf8)
    }
    unpackJobPointer(pointers)
  }

  def decodeBatch(job: AnyBloxJob, tupleStart: Long, tupleCount: Long): FieldVector = {
    val batchAddress = AnyBloxNative.jobRunAndBlock(handle, job.handle, tupleStart, tupleCount)
    val array = ArrowArray.wrap(batchAddress)
    val vector = job.createVector
    Data.importIntoVector(allocator, array, vector, null)
    vector
  }

  def decodeBatch(
      job: AnyBloxJob,
      tupleStart: Long,
      tupleCount: Long,
      outVector: FieldVector): Long = {
    val ts = System.nanoTime()
    val batchAddress = AnyBloxNative.jobRunAndBlock(handle, job.handle, tupleStart, tupleCount)
    val duration = System.nanoTime() - ts
    val array = ArrowArray.wrap(batchAddress)
    Data.importIntoVector(allocator, array, outVector, null)
    duration
  }

  override def close(): Unit = AnyBloxNative.dropRuntime(handle)
}

object AnyBloxRuntime extends AutoCloseable {
  private var instance: Option[AnyBloxRuntime] = None

  def get: AnyBloxRuntime = instance match {
    case Some(r) => r
    case None => throw new AnyBloxRuntimeException("AnyBloxRuntime.init must be called first")
  }

  def init(config: AnyBloxConfig): Unit = {
    instance match {
      case None =>
        instance = Some(new AnyBloxRuntime(AnyBloxNative.createRuntime(config.consume())))
      case Some(_) =>
        throw new AnyBloxRuntimeException("Cannot init an AnyBloxRuntime more than once")
    }
  }

  override def close(): Unit = {
    instance match {
      case None => ()
      case Some(runtime) =>
        runtime.close()
        instance = None
    }
  }
}
