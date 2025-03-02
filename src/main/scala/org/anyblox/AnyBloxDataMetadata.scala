package org.anyblox

case class AnyBloxDataMetadata(
    name: String,
    count: Long,
    sizeInBytes: Option[Long],
    description: Option[String]) {}
