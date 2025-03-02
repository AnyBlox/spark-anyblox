package org.anyblox

class AnyBloxNativeException(message: String, cause: Throwable)
    extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}
