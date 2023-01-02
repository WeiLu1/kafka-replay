package com.kafkareplay.mongo.dao

data class PositionReferenceId(
  val partition: Int?,
  val offset: Long?,
  val topic: String,
  val order: KafkaTopicOrder
)