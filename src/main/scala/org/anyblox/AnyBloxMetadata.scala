package org.anyblox

import org.apache.arrow.vector.types.pojo.Schema

case class AnyBloxMetadata(
    data: AnyBloxDataMetadata,
    decoder: AnyBloxDecoderMetadata,
    schema: Schema) {}
