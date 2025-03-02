package org.anyblox

sealed trait LogLevel {}

object LogLevel {
  case object Trace extends LogLevel

  case object Debug extends LogLevel

  case object Info extends LogLevel

  case object Warning extends LogLevel

  case object Error extends LogLevel

  implicit class LogLevelOps(obj: LogLevel) {
    def asInt: Int = {
      obj match {
        case Trace => 0
        case Debug => 1
        case Info => 2
        case Warning => 3
        case Error => 4
      }
    }
  }
}
