package com.kafkareplay.application.port.usecase

import java.util.*

interface IReplayKafkaMessageUseCase {
  fun invoke(id: UUID)
  fun invoke(topic: String, keys: Set<String?>)
}