package com.kafkareplay.application.port.usecase

import com.kafkareplay.domain.KafkaReplay
import java.util.UUID

interface IGetKafkaReplayMessageUseCase {
  fun invoke(id: UUID): KafkaReplay
}