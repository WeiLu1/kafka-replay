package com.kafkareplay.application.port.usecase

interface IGetAllKafkaReplayKeyUseCase {
  fun invoke(topic: String): Set<String?>
}