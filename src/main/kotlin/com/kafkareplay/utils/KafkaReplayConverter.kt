package com.kafkareplay.utils

import com.kafkareplay.adapter.driver.api.response.KafkaReplayResponse
import com.kafkareplay.adapter.driven.repository.kafkareplay.mongo.KafkaReplayEntity
import java.util.*
import mu.KotlinLogging

object KafkaReplayConverter {
  private val LOG = KotlinLogging.logger {}


  fun convertToResponseDto(kafkaReplayEntity: KafkaReplayEntity): KafkaReplayResponse {
    return KafkaReplayResponse(
      id =  kafkaReplayEntity.id,
      topic = kafkaReplayEntity.topic,
      key = kafkaReplayEntity.key,
      payload = decodeBase64(kafkaReplayEntity.payload),
      exceptionStacktrace = kafkaReplayEntity.exceptionStacktrace
    )
  }

  fun convertToBase64(value: String): String {
    val converted = Base64.getEncoder().encodeToString(value.toByteArray())
    LOG.info("encoded = $value & converted = $converted")
    return converted
  }

  fun decodeBase64(encoded: String): String {
    val decodedBytes = Base64.getDecoder().decode(encoded)
    val decoded = String(decodedBytes)
    LOG.info("encoded = $encoded & decoded = $decoded")
    return decoded
  }
}