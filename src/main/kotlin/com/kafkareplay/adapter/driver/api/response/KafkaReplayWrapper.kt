package com.kafkareplay.adapter.driver.api.response

import io.swagger.v3.oas.annotations.media.Schema

@Schema(description = "Kafka Replay Wrapper")
data class KafkaReplayWrapper(
  @Schema(description = "Ordered List of Kafka Failed Messages")
  val messages: List<KafkaReplayMessage>,
  @Schema(description = "Kafka Key", example = "e454c9da-a6f2-4b90-98ac-f316a1272c05")
  val key: String?
)