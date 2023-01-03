package com.kafkareplay.adapter.driven.repository.kafkareplay.mongo

import com.kafkareplay.domain.KafkaReplay

object KafkaReplayMapper {
  fun toEntity(kafkaReplay: KafkaReplay): KafkaReplayEntity {
    return KafkaReplayEntity(
      id = kafkaReplay.id,
      topic = kafkaReplay.topic,
      key = kafkaReplay.key,
      headers = kafkaReplay.headers,
      exceptionStacktrace = kafkaReplay.exceptionStacktrace,
      payload = kafkaReplay.payload,
      originalPositionReference = PositionReferenceMapper.toEntity(kafkaReplay.positionReference)
    )
  }

  fun toDomain(kafkaReplay: KafkaReplayEntity): KafkaReplay {
    return KafkaReplay(
      id = kafkaReplay.id,
      topic = kafkaReplay.topic,
      key = kafkaReplay.key,
      headers = kafkaReplay.headers,
      exceptionStacktrace = kafkaReplay.exceptionStacktrace,
      payload = kafkaReplay.payload,
      positionReference = PositionReferenceMapper.toDomain(kafkaReplay.originalPositionReference)
    )
  }
}