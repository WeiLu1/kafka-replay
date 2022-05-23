package com.kafkareplay.model.dto

import java.util.*

data class KafkaReplayDto (
  val id: UUID,
  val topic: String,
  val key: String,
  val payload: String,
  val exceptionStacktrace: String,
  )