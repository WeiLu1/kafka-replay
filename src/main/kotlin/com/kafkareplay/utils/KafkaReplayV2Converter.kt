package com.kafkareplay.utils

import com.kafkareplay.model.dto.KafkaReplayMessage
import com.kafkareplay.model.dto.PositionReference
import com.kafkareplay.mongo.dao.KafkaReplayDao
import com.kafkareplay.mongo.dao.PositionReferenceId
import java.util.*
import mu.KotlinLogging

object KafkaReplayV2Converter {
  private val LOG = KotlinLogging.logger {}


  fun convertToResponseDto(kafkaReplayDao: KafkaReplayDao): KafkaReplayMessage {
    return KafkaReplayMessage(
      id =  kafkaReplayDao.id,
      topic = kafkaReplayDao.topic,
      key = kafkaReplayDao.key,
      payload = decodeBase64(kafkaReplayDao.payload),
      exceptionStacktrace = kafkaReplayDao.exceptionStacktrace,
      positionReference = convertToPositionReference(kafkaReplayDao.originalPositionReference)
    )
  }

  fun convertToPositionReference(positionReferenceId: PositionReferenceId): PositionReference {
    return PositionReference(
      positionReferenceId.partition,
      positionReferenceId.offset,
      positionReferenceId.topic
    )
  }

  fun convertToBase64(value: String): String {
    val converted = Base64.getEncoder().encodeToString(value.toByteArray())
    LOG.info("encoded = $value & converted = $converted")
    return converted
  }

  fun decodeByteArray(encoded: String): ByteArray {
    val decodedBytes = Base64.getDecoder().decode(encoded)
    return decodedBytes
  }
  fun decodeBase64(encoded: String): String {
    val decodedBytes = Base64.getDecoder().decode(encoded)
    val decoded = String(decodedBytes)
    LOG.info("encoded = $encoded & decoded = $decoded")
    return decoded
  }
}