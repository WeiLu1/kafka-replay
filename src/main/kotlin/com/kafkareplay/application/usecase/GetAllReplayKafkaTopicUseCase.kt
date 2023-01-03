package com.kafkareplay.application.usecase

import com.kafkareplay.application.port.adapter.driven.repository.kafkareplay.IKafkaReplayRepository
import com.kafkareplay.application.port.usecase.IGetAllKafkaReplayTopicUseCase

class GetAllReplayKafkaTopicUseCase(
  private val kafkaReplayRepository: IKafkaReplayRepository
): IGetAllKafkaReplayTopicUseCase {
  override fun invoke(): Set<String> {
    return kafkaReplayRepository
      .getAll()
      .map { it.topic }
      .distinct()
      .toSet()
  }
}