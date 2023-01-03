package com.kafkareplay.application.service

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.service.IKafkaReplayService
import com.kafkareplay.commons.exception.KafkaOriginalPositionReferenceNotFoundException
import com.kafkareplay.commons.exception.KafkaReplayNotFoundException
import com.kafkareplay.domain.KafkaReplay
import com.kafkareplay.domain.KafkaTopicOrder
import java.util.UUID

class KafkaReplayService(
  private val kafkaReplayRepository: IKafkaReplayRepository,
): IKafkaReplayService {
  override fun findPreviousMessage(currentId: UUID): KafkaReplay? {
    val currentMessage = kafkaReplayRepository.getById(currentId) ?: throw KafkaReplayNotFoundException(currentId)
    validatedMessages(mapOf(currentMessage.key to listOf(currentMessage)))
    if (currentMessage.positionReference.order == KafkaTopicOrder.SORTED) {
      return kafkaReplayRepository.getPreviousReplayMessage(
        currentMessage.key,
        currentMessage.topic,
        currentMessage.positionReference.partition!!,
        currentMessage.positionReference.offset!!
      )
    }

    return null
  }

  override fun deleteByTopicAndKeys(topic: String, keys: Set<String?>) {
    keys.forEach { key -> kafkaReplayRepository.deleteByTopicAndKey(topic, key) }
  }

  override fun findAll(topic: String, keys: Set<String?>): Map<String?, List<KafkaReplay>> {
    val messages = keys.associateWith {
        key -> kafkaReplayRepository.getAllByTopicAndKeys(topic, key)
    }
    validatedMessages(messages)
    return messages
  }

  private fun validatedMessages(messages: Map<String?, List<KafkaReplay>>) {
    messages.keys.forEach {
      messages[it]?.forEach { message ->
        if (message.positionReference.order == KafkaTopicOrder.SORTED && (message.positionReference.partition == null || message.positionReference.offset == null)) {
          throw KafkaOriginalPositionReferenceNotFoundException(message.id)
        }
      }
    }
  }
}