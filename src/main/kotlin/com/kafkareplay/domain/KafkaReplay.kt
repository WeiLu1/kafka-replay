package com.kafkareplay.domain

import java.util.*

data class KafkaReplay(
  val id: UUID,
  val topic: String,
  val key: String?,
  val payload: String,
  val exceptionStacktrace: String,
  val positionReference: OriginalPositionReference,
  val headers: Map<String, ByteArray>
)
