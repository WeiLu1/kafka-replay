package com.kafkareplay.adapter.driver.api.response

import io.swagger.v3.oas.annotations.media.Schema

@Schema(description = "Kafka Position Reference")
data class PositionReference(
  @Schema(description = "Partition", example = "1")
  val partition: Int?,
  @Schema(description = "Offset", example = "26")
  val offset: Long?,
  @Schema(description = "Kafka Target Topic", example = "some-topic_ms-kafka-failure_RETRY")
  val topic: String
)