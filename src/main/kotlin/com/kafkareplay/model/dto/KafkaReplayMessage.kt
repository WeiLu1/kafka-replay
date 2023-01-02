package com.kafkareplay.model.dto

import com.fasterxml.jackson.annotation.JsonProperty
import io.swagger.v3.oas.annotations.media.Schema
import java.util.*

@Schema(description = "Kafka Replay Message")
data class KafkaReplayMessage(

  @JsonProperty("id")
  @Schema(description = "Replay ID", example = "e454c9da-a6f2-4b90-98ac-f316a1272c05")
  val id: UUID,

  @JsonProperty("topic")
  @Schema(description = "Kafka Target Topic", example = "some-topic_ms-kafka-failure_RETRY")
  val topic: String,

  @JsonProperty("key")
  @Schema(description = "Kafka Key", example = "e454c9da-a6f2-4b90-98ac-f316a1272c05")
  val key: String?,

  @JsonProperty("payload")
  @Schema(description = "Kafka Payload", example = "{ \"name\": \"tester\"}")
  val payload: String,

  @JsonProperty("exceptionStacktrace")
  @Schema(description = "Kafka Payload", example = "org.springframework.kafka.listener.ListenerExecutionFailedException: Listener method 'public void com.ak.ms.kafkafailure.kafka.consumer.MaintainingOrderKafkaC...")
  val exceptionStacktrace: String,

  @JsonProperty("positionReference")
  @Schema(description = "Position of message in the original Topic where it failed to consume")
  val positionReference: PositionReference
)