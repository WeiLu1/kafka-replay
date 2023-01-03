package com.kafkareplay.application.port.usecase

import com.kafkareplay.domain.KafkaReplay

interface IGetAllKafkaReplayMessagesUseCase {
  fun invoke(topic: String, keys: Set<String?>): Map<String?, List<KafkaReplay>>
}