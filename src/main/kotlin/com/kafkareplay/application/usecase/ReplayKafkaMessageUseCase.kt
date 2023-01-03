package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.service.IKafkaReplayService
import com.kafkareplay.application.port.usecase.IReplayKafkaMessageUseCase
import com.kafkareplay.commons.exception.KafkaReplayNotFoundException
import com.kafkareplay.commons.exception.UnableToReplayKafkaMessageException
import com.kafkareplay.utils.KafkaReplayConverter
import com.kafkareplay.utils.KafkaReplayV2Converter
import java.util.*

class ReplayKafkaMessageUseCase(
  private val kafkaReplayService: IKafkaReplayService,
  private val kafkaReplayRepository: IKafkaReplayRepository,
  private val retryTopicSender: IRetryTopicSender
): IReplayKafkaMessageUseCase {

  override fun invoke(id: UUID) {
    val currentMessage = kafkaReplayRepository.getById(id) ?: throw KafkaReplayNotFoundException(id)
    kafkaReplayService.findPreviousMessage(id)?.run {
      throw UnableToReplayKafkaMessageException(id, currentMessage.id)
    }

    retryTopicSender.send(currentMessage.topic, currentMessage.key, KafkaReplayConverter.decodeBase64(currentMessage.payload), currentMessage.headers)
    kafkaReplayRepository.deleteById(id)
  }

  override fun invoke(topic: String, keys: Set<String?>) {
    val orderedMessages = kafkaReplayService.findAll(topic, keys)
    orderedMessages.keys.forEach { key ->
      orderedMessages[key]?.forEach { message ->
        retryTopicSender.send(topic, key, KafkaReplayV2Converter.decodeBase64(message.payload), message.headers)
      }

      kafkaReplayService.deleteByTopicAndKeys(topic, keys)
    }
  }
}