package com.kafkareplay.adapter.driver.api.request

import io.swagger.v3.oas.annotations.media.Schema
import jakarta.validation.constraints.NotEmpty
import jakarta.validation.constraints.NotNull

@Schema(description = "Kafka Replay Request")
data class ReplayRequest(
  @field:NotNull
  @Schema(description = "Kafka Topic", example = "some-topic_ms-kafka-failure_RETRY", required = true)
  val topic: String,
  @field:NotNull
  @field:NotEmpty
  @Schema(description = "Kafka Keys", example = """["1234567", "e454c9da-a6f2-4b90-98ac-f316a1272c05", null]""", required = true)
  val keys: Set<String?>
)
