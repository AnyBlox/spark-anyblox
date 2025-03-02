package org.anyblox

case class AnyBloxDecoderMetadata(
    name: String,
    description: Option[String],
    license: Option[String],
    checksum_blake3: Option[String],
    min_batch_size: Option[Long]) {}
