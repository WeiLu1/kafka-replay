package com.kafkareplay.utils

import com.kafkareplay.model.dto.KafkaReplayResponse
import com.kafkareplay.mongo.dao.KafkaReplayDao
import java.util.*
import mu.KotlinLogging

object KafkaReplayConverter {
  private val LOG = KotlinLogging.logger {}


  fun convertToResponseDto(kafkaReplayDao: KafkaReplayDao): KafkaReplayResponse {
    return KafkaReplayResponse(
      id =  kafkaReplayDao.id,
      topic = kafkaReplayDao.topic,
      key = kafkaReplayDao.key,
      payload = decodeBase64(kafkaReplayDao.payload),
      exceptionStacktrace = kafkaReplayDao.exceptionStacktrace
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