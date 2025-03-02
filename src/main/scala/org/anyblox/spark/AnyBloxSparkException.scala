package org.anyblox.spark

class AnyBloxSparkException(message: String, cause: Throwable)
    extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}
