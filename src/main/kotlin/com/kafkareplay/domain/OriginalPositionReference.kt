package com.kafkareplay.domain

data class OriginalPositionReference(
  val partition: Int?,
  val offset: Long?,
  val topic: String,
  val order: KafkaTopicOrder
)