package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.usecase.IGetKafkaReplayMessageUseCase
import com.kafkareplay.commons.exception.KafkaReplayNotFoundException
import com.kafkareplay.domain.KafkaReplay
import java.util.UUID

class GetReplayKafkaMessageUseCase(
  private val kafkaReplayRepository: IKafkaReplayRepository
): IGetKafkaReplayMessageUseCase {
  override fun invoke(id: UUID): KafkaReplay {
    return kafkaReplayRepository.getById(id) ?: throw KafkaReplayNotFoundException(id)
  }
}