package com.kafkareplay.adapter.driver.api.mapper

import com.kafkareplay.adapter.driver.api.response.KafkaReplayMessage
import com.kafkareplay.adapter.driver.api.response.PositionReference
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayEntity
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.PositionReferenceEntity
import com.kafkareplay.domain.KafkaReplay
import com.kafkareplay.domain.OriginalPositionReference
import com.kafkareplay.utils.KafkaReplayConverter.decodeBase64
import java.util.*
import mu.KotlinLogging

object KafkaReplayV2Converter {
  private val LOG = KotlinLogging.logger {}


  fun convertToResponseDto(kafkaReplay: KafkaReplay): KafkaReplayMessage {
    return KafkaReplayMessage(
      id =  kafkaReplay.id,
      topic = kafkaReplay.topic,
      key = kafkaReplay.key,
      payload = decodeBase64(kafkaReplay.payload),
      exceptionStacktrace = kafkaReplay.exceptionStacktrace,
      positionReference = convertToPositionReference(kafkaReplay.positionReference)
    )
  }

  fun convertToPositionReference(originalPositionReference: OriginalPositionReference): PositionReference {
    return PositionReference(
      originalPositionReference.partition,
      originalPositionReference.offset,
      originalPositionReference.topic
    )
  }
}