package com.kafkareplay.adapter.driven.repository.kafkareplay.mongo

import com.kafkareplay.domain.KafkaTopicOrder

data class PositionReferenceEntity(
  val partition: Int?,
  val offset: Long?,
  val topic: String,
  val order: KafkaTopicOrder
)