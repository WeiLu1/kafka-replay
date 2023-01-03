package com.kafkareplay.adapter.driven.repository.kafkareplay.mongo

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.util.*

@Document("failed-messages")
data class KafkaReplayEntity (
  @Id
  val id: UUID,
  val originalPositionReference: PositionReferenceEntity,
  val topic: String,
  val key: String?,
  val payload: String,
  val exceptionStacktrace: String,
  val headers: Map<String, ByteArray>
)
