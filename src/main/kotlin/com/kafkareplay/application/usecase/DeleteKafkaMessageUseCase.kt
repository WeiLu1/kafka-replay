package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.event.sender.IRetryTopicSender
import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.service.IKafkaReplayService
import com.kafkareplay.application.port.usecase.IDeleteKafkaMessageUseCase
import com.kafkareplay.application.port.usecase.IReplayKafkaMessageUseCase
import com.kafkareplay.commons.exception.KafkaReplayNotFoundException
import com.kafkareplay.commons.exception.UnableToDeleteKafkaMessageException
import com.kafkareplay.commons.exception.UnableToReplayKafkaMessageException
import com.kafkareplay.utils.KafkaReplayConverter
import com.kafkareplay.utils.KafkaReplayV2Converter
import java.util.*

class DeleteKafkaMessageUseCase(
  private val kafkaReplayService: IKafkaReplayService,
  private val kafkaReplayRepository: IKafkaReplayRepository
): IDeleteKafkaMessageUseCase {

  override fun invoke(id: UUID) {
    val currentMessage = kafkaReplayRepository.getById(id) ?: throw KafkaReplayNotFoundException(id)
    kafkaReplayService.findPreviousMessage(id)?.run {
      throw UnableToDeleteKafkaMessageException(id, currentMessage.id)
    }

    kafkaReplayRepository.deleteById(id)
  }

  override fun invoke(topic: String, keys: Set<String?>) {
    kafkaReplayService.deleteByTopicAndKeys(topic, keys)
  }
}