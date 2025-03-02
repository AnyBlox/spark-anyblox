package org.anyblox

import scala.jdk.CollectionConverters

import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.complex.StructVector

class AnyBloxTests extends munit.FunSuite {
  test("sanity") {
    val configBuilder = new AnyBloxConfigBuilder
    configBuilder.setWasmCacheLimit(64 * 1024 * 1024).setLogLevel(LogLevel.Trace)
    val config = configBuilder.build()
    val runtime = new AnyBloxRuntime(config)

    val bundle = AnyBloxBundle.openExtensionAndData(
      "/home/mat/src/portable-decompress/dataset/Bimbo-rle-ext.anyblox",
      "/home/mat/src/portable-decompress/dataset/1_Agencia_ID.rle")
    val metadata = bundle.metadata

    val job = runtime.initBlockingJob(bundle)
    var remTuples = metadata.data.count
    var tupleStart = 0
    var groups: Map[Int, Int] = Map.empty

    while (remTuples > 0) {
      val request = Math.min(remTuples, 100000)
      val batch = runtime.decodeBatch(job, tupleStart, request)

      batch match {
        case struct: StructVector =>
          val column = struct.getVectorById(0)
          column match {
            case c: IntVector =>
              for (i <- 0 until c.getValueCount) {
                val x = c.get(i)
                groups = groups.updatedWith(x) {
                  case Some(cnt) => Some(cnt + 1)
                  case None => Some(1)
                }
              }
            case _ => throw new ClassCastException()
          }
        case _ => throw new ClassCastException
      }

      tupleStart += batch.getValueCount
      remTuples -= batch.getValueCount
    }
    assertEquals(groups.get(0), None)
    assertEquals(groups.get(1111), Some(449195))
  }
}
