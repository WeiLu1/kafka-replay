package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.usecase.ISaveKafkaErrorEventUseCase
import com.kafkareplay.commons.helper.Base64Converter
import com.kafkareplay.domain.KafkaReplay
import com.kafkareplay.domain.KafkaTopicOrder
import com.kafkareplay.domain.OriginalPositionReference
import java.util.*
import mu.KotlinLogging

class SaveKafkaErrorEventUseCase(
  private val kafkaReplayRepository: IKafkaReplayRepository
): ISaveKafkaErrorEventUseCase {

  companion object {
    private val LOG = KotlinLogging.logger {}
  }

  override fun invoke(topic: String, key: String?, payload: String, exceptionStacktrace: String, headers: Map<String, ByteArray>) {
    try {
      val uuid = UUID.randomUUID()
      val referenceId = headers["RETRYING_REFERENCE_ID"]?.let { String(it).split(":") }
      val order = headers["RETRYING_ORDER"]?.let {
        if (String(it) == "SORTED") KafkaTopicOrder.SORTED else KafkaTopicOrder.UNSORTED
      } ?: KafkaTopicOrder.UNSORTED

      val originalPartition = referenceId?.get(0)?.toInt()
      val originalOffset = referenceId?.get(1)?.toLong()
      val targetTopic = topic.replace("_ERROR", "_RETRY")
      val positionReference = OriginalPositionReference(originalPartition, originalOffset, targetTopic, order)

      val kafkaReplay = KafkaReplay(
        id = uuid,
        topic = targetTopic,
        key = key,
        payload = Base64Converter.convertToBase64(payload),
        exceptionStacktrace = exceptionStacktrace,
        headers = headers,
        positionReference = positionReference
      )
      kafkaReplayRepository.save(kafkaReplay)
    } catch (e: Exception) {
      LOG.warn("Failed to save message for topic:[$topic] and key:[$key]")
      throw e
    }
  }
}