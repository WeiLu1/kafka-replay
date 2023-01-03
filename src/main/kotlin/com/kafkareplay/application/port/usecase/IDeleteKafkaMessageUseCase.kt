package com.kafkareplay.application.port.usecase

import java.util.*

interface IDeleteKafkaMessageUseCase {
  fun invoke(id: UUID)
  fun invoke(topic: String, keys: Set<String?>)
}