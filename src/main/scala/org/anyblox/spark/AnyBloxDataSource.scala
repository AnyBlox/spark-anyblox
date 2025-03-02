package org.anyblox.spark

import java.util

import org.anyblox.AnyBloxBundle

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class AnyBloxDataSource
    extends TableProvider
    with DataSourceRegister
    with Logging
    with AutoCloseable {
  private var table: Option[AnyBloxTable] = None
  private val sparkSession = SparkSession.active

  logInfo("Instance of AnyBloxDataSource created")

  override def shortName(): String = "anyblox"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    table match {
      case Some(t) => t.schema()
      case None =>
        if (!options.containsKey("path")) {
          throw new AnyBloxSparkException("no path in options to AnyBloxDataSource")
        }

        val anybloxPath = options.get("path")

        val (key, bundle) = if (options.containsKey("data")) {
          val dataPath = options.get("data")
          val key = AnyBloxExtensionBundleKey(anybloxPath, dataPath)
          val bundle = AnyBloxBundle.openExtensionAndData(anybloxPath, dataPath)
          (key, bundle)
        } else {
          val key = AnyBloxSelfContainedBundleKey(anybloxPath)
          val bundle = AnyBloxBundle.openSelfContained(anybloxPath)
          (key, bundle)
        }

        val t = AnyBloxTable.takeBundle(sparkSession, key, bundle)
        table = Some(t)
        t.schema()
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    logInfo("partitioning: " + partitioning.mkString("Array(", ", ", ")"))
    logInfo("properties: " + properties)
    table match {
      case Some(m) => m
      case None =>
        throw new AnyBloxSparkException(
          "getTable called without inferSchema first, this should not happen")
    }
  }

  override def inferPartitioning(options: CaseInsensitiveStringMap): Array[Transform] =
    super.inferPartitioning(options)

  override def close(): Unit = table match {
    case Some(t) => t.close()
    case None =>
  }
}
