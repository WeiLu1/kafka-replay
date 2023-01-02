package com.kafkareplay.model.dto

import io.swagger.v3.oas.annotations.media.Schema

data class ErrorResponse(
  @Schema(description = "Error Code", example = "MS_KAFA_REPLAY-000", required = true)
  val errorCode: String,
  @Schema(description = "Error Message", example = "Kafka Replay not found for ID:[e454c9da-a6f2-4b90-98ac-f316a1272c05]", required = false)
  val errorMessage: String?,
  @Schema(description = "Trace ID", example = "63b2ca5d4516d8c315af7ed7dcd1c592_15af7ed7dcd1c592", required = false)
  val traceId: String?
)
