package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.usecase.IGetAllKafkaReplayMessagesUseCase
import com.kafkareplay.domain.KafkaReplay

class GetAllReplayKafkaMessagesUseCase(
  private val kafkaReplayRepository: IKafkaReplayRepository
): IGetAllKafkaReplayMessagesUseCase {
  override fun invoke(topic: String, keys: Set<String?>): Map<String?, List<KafkaReplay>> {
    return keys.associateWith {
        key -> kafkaReplayRepository.getAllByTopicAndKeys(topic, key)
    }
  }
}