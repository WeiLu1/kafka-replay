package com.kafkareplay.model.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

data class KafkaReplayResponse (
  @JsonProperty("id")
  val id: UUID,

  @JsonProperty("topic")
  val topic: String,

  @JsonProperty("key")
  val key: String,

  @JsonProperty("payload")
  val payload: String,

  @JsonProperty("exceptionStacktrace")
  val exceptionStacktrace: String,
  )