package org.anyblox.spark

import org.anyblox.{AnyBloxConfigBuilder, AnyBloxRuntime}

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.internal.Logging

class AnyBloxDriverPlugin extends DriverPlugin with Logging {
  AnyBloxRuntime.init(AnyBloxConfigBuilder.default.build())

  override def shutdown(): Unit = {
    logInfo("AnyBloxDriverPlugin shutdown")

    AnyBloxRuntime.close()
    super.shutdown()
  }
}

class AnyBloxPlugin extends SparkPlugin with Logging {
  override def driverPlugin(): DriverPlugin = new AnyBloxDriverPlugin

  override def executorPlugin(): ExecutorPlugin = null
}
