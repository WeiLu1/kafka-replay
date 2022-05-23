package com.kafkareplay.mongo.dao

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.util.*

@Document("failed-messages")
data class KafkaReplayDao (
  @Id
  val id: UUID,
  val topic: String,
  val key: String,
  val payload: String,
  val exceptionStacktrace: String,
)
