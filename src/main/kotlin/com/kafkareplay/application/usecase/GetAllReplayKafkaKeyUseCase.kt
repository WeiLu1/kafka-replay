package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.usecase.IGetAllKafkaReplayKeyUseCase

class GetAllReplayKafkaKeyUseCase(
  private val kafkaReplayRepository: IKafkaReplayRepository
): IGetAllKafkaReplayKeyUseCase {
  override fun invoke(topic: String): Set<String?> {
    return kafkaReplayRepository
      .getAllByTopic(topic)
      .map { it.key }
      .distinct()
      .toSet()
  }
}