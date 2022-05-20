package com.kafkareplay.mongo.dao

import org.springframework.data.annotation.Id
import java.util.*

data class KafkaReplayDao (
  @Id
  val id: UUID,
  val topic: String,
  val paylaod: String,
  val exceptionStacktrace: String,
)
